package com.ichi2.libanki

import android.database.Cursor
import androidx.sqlite.db.SupportSQLiteStatement
import com.ichi2.anki.AnkiDroidApp
import com.ichi2.anki.R
import com.ichi2.anki.analytics.UsageAnalytics
import com.ichi2.async.DeckTask.ProgressCallback
import com.ichi2.async.DeckTask.TaskData
import com.ichi2.libanki.exception.NoSuchDeckException
import java.io.File
import java.util.Locale
import org.json.JSONException
import org.json.JSONObject
import timber.log.Timber

// The Java code currently only expects a long value only as a reply. In the future
// it would make sense to pass back the IntegrityCheckOutput object, and display the
// problems in a popup.
fun fixIntegritySizeOnly(col: Collection, progressCallback: ProgressCallback): Long {
    val out = IntegrityCheck(col, progressCallback).fixIntegrity()
    return out?.bytesSaved ?: -1
}

data class IntegrityCheckOutput(val problems: ArrayList<String>, val bytesSaved: Long)

private class IntegrityCheck(val col: Collection, val progressCallback: ProgressCallback) {
    val problems = ArrayList<String>()
    var currentTask = 0
    var totalTasks: Int

    init {
        // fixme: this number needs updating
        totalTasks = col.models.all().size * 4 + 23 // a few fixes are in all-models loops, the rest are one-offs
    }

    private fun notifyProgress() {
        progressCallback.publishProgress(TaskData(
                progressCallback.resources.getString(R.string.check_db_message).toString() + " " + currentTask++ + " / " + totalTasks))
    }

    private fun runCheck(check: () -> Unit) {
        // DEFECT: notifyProgress will lag if an exception is thrown.
        try {
            notifyProgress()
            col.db.getDatabase().beginTransaction()
            check()
            col.db.getDatabase().setTransactionSuccessful()
        } catch (e: Exception) {
            Timber.e(e, "Failed to execute integrity check")
            AnkiDroidApp.sendExceptionReport(e, "fixIntegrity")
        } finally {
            try {
                col.db.getDatabase().endTransaction()
            } catch (e: Exception) {
                Timber.e(e, "Failed to end integrity check transaction")
                AnkiDroidApp.sendExceptionReport(e, "fixIntegrity - endTransaction")
            }
        }
    }

    fun fixIntegrity(): IntegrityCheckOutput? {
        var file = File(col.path)
        val oldSize: Long = file.length()

        try {
            col.db.getDatabase().beginTransaction()
            col.save()
            notifyProgress()
            if (!col.db.getDatabase().isDatabaseIntegrityOk()) {
                return null
            }
            col.db.getDatabase().setTransactionSuccessful()
        } catch (e: RuntimeException) {
            Timber.e(e, "doInBackgroundCheckDatabase - RuntimeException on marking card")
            AnkiDroidApp.sendExceptionReport(e, "doInBackgroundCheckDatabase")
            return null
        } finally {
            col.db.getDatabase().endTransaction()
        }

        runCheck(::deleteNotesWithMissingModel)
        // for each model
        for (m in col.models.all()) {
            runCheck({ deleteCardsWithInvalidModelOrdinals(m) })
            runCheck({ deleteNotesWithWrongFieldCounts(m) })
        }
        runCheck(::deleteNotesWithMissingCards)
        runCheck(::deleteCardsWithMissingNotes)
        runCheck(::removeOriginalDuePropertyWhereInvalid)
        runCheck(::removeDynamicPropertyFromNonDynamicDecks)
        runCheck(::removeDeckOptionsFromDynamicDecks)
        runCheck(::rebuildTags)
        runCheck(::updateFieldCache)
        runCheck(::fixNewCardDuePositionOverflow)
        runCheck(::resetNewCardInsertionPosition)
        runCheck(::fixExcessiveReviewDueDates)
        // v2 sched had a bug that could create decimal intervals
        runCheck(::fixDecimalCardsData)
        runCheck(::fixDecimalRevLogData)
        runCheck(::restoreMissingDatabaseIndices)
        runCheck(::ensureModelsAreNotEmpty)

        // and finally, optimize (unable to be done inside transaction).
        try {
            optimize()
        } catch (e: Exception) {
            Timber.e(e, "optimize")
            AnkiDroidApp.sendExceptionReport(e, "fixIntegrity - optimize")
        }
        file = File(col.path)
        val newSize: Long = file.length()
        // if any problems were found, force a full sync
        if (problems.size > 0) {
            col.modSchemaNoCheck()
        }

        logProblems(problems)

        return IntegrityCheckOutput(problems, (oldSize - newSize) / 1024)
    }

    private fun ensureModelsAreNotEmpty() {
        Timber.d("ensureModelsAreNotEmpty()")
        if (col.models.ensureNotEmpty()) {
            problems.add("Added missing note type.")
        }
    }

    private fun restoreMissingDatabaseIndices() {
        Timber.d("restoreMissingDatabaseIndices")
        // DB must have indices. Older versions of AnkiDroid didn't create them for new collections.
        val ixs: Int = col.db.queryScalar("select count(name) from sqlite_master where type = 'index'")
        if (ixs < 7) {
            problems.add("Indices were missing.")
            Storage.addIndices(col.db)
        }
    }

    private fun fixDecimalCardsData() {
        Timber.d("fixDecimalCardsData")
        val s: SupportSQLiteStatement = col.db.getDatabase().compileStatement(
                "update cards set ivl=round(ivl),due=round(due) where ivl!=round(ivl) or due!=round(due)")
        val rowCount = s.executeUpdateDelete()
        if (rowCount > 0) {
            problems.add("Fixed $rowCount cards with v2 scheduler bug.")
        }
    }

    private fun fixDecimalRevLogData() {
        Timber.d("fixDecimalRevLogData()")
        val s: SupportSQLiteStatement = col.db.getDatabase().compileStatement(
                "update revlog set ivl=round(ivl),lastIvl=round(lastIvl) where ivl!=round(ivl) or lastIvl!=round(lastIvl)")
        val rowCount = s.executeUpdateDelete()
        if (rowCount > 0) {
            problems.add("Fixed $rowCount review history entries with v2 scheduler bug.")
        }
    }

    private fun fixExcessiveReviewDueDates() {
        Timber.d("fixExcessiveReviewDueDates()")
        // reviews should have a reasonable due #
        val ids: ArrayList<Long> = col.db.queryColumn(Long::class.java, "SELECT id FROM cards WHERE queue = 2 AND due > 100000", 0)
        if (ids.size > 0) {
            problems.add("Reviews had incorrect due date.")
            col.db.execute("UPDATE cards SET due = " + col.sched.getToday() + ", ivl = 1, mod = " + Utils.intTime() +
                    ", usn = " + col.usn() + " WHERE id IN " + Utils.ids2str(Utils.arrayList2array(ids)))
        }
    }

    @Throws(JSONException::class)
    private fun resetNewCardInsertionPosition() {
        Timber.d("resetNewCardInsertionPosition")
        // new card position
        col.conf.put("nextPos", col.db.queryScalar("SELECT max(due) + 1 FROM cards WHERE type = 0"))
    }

    private fun fixNewCardDuePositionOverflow() {
        Timber.d("fixNewCardDuePositionOverflow")
        // new cards can't have a due position > 32 bits
        col.db.execute("UPDATE cards SET due = 1000000, mod = " + Utils.intTime() + ", usn = " + col.usn() +
                " WHERE due > 1000000 AND type = 0")
    }

    private fun updateFieldCache() {
        Timber.d("updateFieldCache")
        // field cache
        for (m in col.models.all()) {
            notifyProgress()
            col.updateFieldCache(Utils.arrayList2array(col.models.nids(m)))
        }
    }

    private fun rebuildTags() {
        Timber.d("rebuildTags")
        // tags
        col.tags.registerNotes()
    }

    private fun removeDeckOptionsFromDynamicDecks() {
        Timber.d("removeDeckOptionsFromDynamicDecks()")
        // #5708 - a dynamic deck should not have "Deck Options"
        var fixCount = 0
        for (id in col.decks.allDynamicDeckIds()) {
            try {
                if (col.decks.hasDeckOptions(id)) {
                    col.decks.removeDeckOptions(id)
                    fixCount++
                }
            } catch (e: NoSuchDeckException) {
                Timber.e("Unable to find dynamic deck %d", id)
            }
        }
        if (fixCount > 0) {
            col.decks.save()
            problems.add(String.format(Locale.US, "%d dynamic deck(s) had deck options.", fixCount))
        }
    }

    private fun removeDynamicPropertyFromNonDynamicDecks() {
        Timber.d("removeDynamicPropertyFromNonDynamicDecks()")
        val dids = ArrayList<Long>()
        for (id in col.decks.allIds()) {
            if (!col.decks.isDyn(id)) {
                dids.add(id)
            }
        }
        // cards with odid set when not in a dyn deck
        val ids: ArrayList<Long> = col.db.queryColumn(Long::class.java,
                "select id from cards where odid > 0 and did in " + Utils.ids2str(dids), 0)
        if (ids.size != 0) {
            problems.add("Fixed " + ids.size + " card(s) with invalid properties.")
            col.db.execute("update cards set odid=0, odue=0 where id in " + Utils.ids2str(ids))
        }
    }

    private fun removeOriginalDuePropertyWhereInvalid() {
        Timber.d("removeOriginalDuePropertyWhereInvalid()")
        // cards with odue set when it shouldn't be
        val ids: ArrayList<Long> = col.db.queryColumn(Long::class.java,
                "select id from cards where odue > 0 and (type=1 or queue=2) and not odid", 0)
        if (ids.size != 0) {
            problems.add("Fixed " + ids.size + " card(s) with invalid properties.")
            col.db.execute("update cards set odue=0 where id in " + Utils.ids2str(ids))
        }
    }

    private fun deleteCardsWithMissingNotes() {
        Timber.d("deleteCardsWithMissingNotes()")
        val ids: ArrayList<Long> // cards with missing notes
        ids = col.db.queryColumn(Long::class.java,
                "SELECT id FROM cards WHERE nid NOT IN (SELECT id FROM notes)", 0)
        if (ids.size != 0) {
            problems.add("Deleted " + ids.size + " card(s) with missing note.")
            col.remCards(Utils.arrayList2array(ids))
        }
    }

    private fun deleteNotesWithMissingCards() {
        Timber.d("deleteNotesWithMissingCards()")
        val ids: ArrayList<Long>
        // delete any notes with missing cards
        ids = col.db.queryColumn(Long::class.java,
                "SELECT id FROM notes WHERE id NOT IN (SELECT DISTINCT nid FROM cards)", 0)
        if (ids.size != 0) {
            problems.add("Deleted " + ids.size + " note(s) with missing no cards.")
            col._remNotes(Utils.arrayList2array(ids))
        }
    }

    @Throws(JSONException::class)
    private fun deleteNotesWithWrongFieldCounts(m: JSONObject) {
        Timber.d("deleteNotesWithWrongFieldCounts")
        val ids: ArrayList<Long> // notes with invalid field counts
        ids = ArrayList()
        var cur: Cursor? = null
        try {
            cur = col.db.getDatabase().query("select id, flds from notes where mid = " + m.getLong("id"), null)
            Timber.i("cursor size: %d", cur.count)
            var currentRow = 0

            // Since we loop through all rows, we only want one exception
            var firstException: Exception? = null
            while (cur.moveToNext()) {
                try {
                    val flds = cur.getString(1)
                    val id = cur.getLong(0)
                    var fldsCount = 0
                    for (i in 0 until flds.length) {
                        if (flds[i].toInt() == 0x1f) {
                            fldsCount++
                        }
                    }
                    if (fldsCount + 1 != m.getJSONArray("flds").length()) {
                        ids.add(id)
                    }
                } catch (ex: IllegalStateException) {
                    // DEFECT: Theory that is this an OOM is discussed in #5852
                    // We store one exception to stop excessive logging
                    Timber.i(ex, "deleteNotesWithWrongFieldCounts - Exception on row %d. Columns: %d", currentRow, cur.columnCount)
                    if (firstException == null) {
                        val details = String.format(Locale.ROOT, "deleteNotesWithWrongFieldCounts row: %d col: %d",
                                currentRow,
                                cur.columnCount)
                        AnkiDroidApp.sendExceptionReport(ex, details)
                        firstException = ex
                    }
                }
                currentRow++
            }
            Timber.i("deleteNotesWithWrongFieldCounts - completed successfully")
            notifyProgress()
            if (ids.size > 0) {
                problems.add("Deleted " + ids.size + " note(s) with wrong field count.")
                col._remNotes(Utils.arrayList2array(ids))
            }
        } finally {
            if (cur != null && !cur.isClosed) {
                cur.close()
            }
        }
    }

    @Throws(JSONException::class)
    private fun deleteCardsWithInvalidModelOrdinals(m: JSONObject) {
        Timber.d("deleteCardsWithInvalidModelOrdinals()")
        if (m.getInt("type") == Consts.MODEL_STD) {
            val ords = ArrayList<Int>()
            val tmpls = m.getJSONArray("tmpls")
            for (t in 0 until tmpls.length()) {
                ords.add(tmpls.getJSONObject(t).getInt("ord"))
            }
            // cards with invalid ordinal
            val ids: ArrayList<Long> = col.db.queryColumn(Long::class.java,
                    "SELECT id FROM cards WHERE ord NOT IN " + Utils.ids2str(ords) + " AND nid IN ( " +
                            "SELECT id FROM notes WHERE mid = " + m.getLong("id") + ")", 0)
            if (ids.size > 0) {
                problems.add("Deleted " + ids.size + " card(s) with missing template.")
                col.remCards(Utils.arrayList2array(ids))
            }
        }
    }

    private fun deleteNotesWithMissingModel() {
        Timber.d("deleteNotesWithMissingModel()")
        // note types with a missing model
        val ids: ArrayList<Long> = col.db.queryColumn(Long::class.java,
                "SELECT id FROM notes WHERE mid NOT IN " + Utils.ids2str(col.models.ids()), 0)
        if (ids.size != 0) {
            problems.add("Deleted " + ids.size + " note(s) with missing note type.")
            col._remNotes(Utils.arrayList2array(ids))
        }
    }

    fun optimize() {
        Timber.i("executing VACUUM ANALYZE statement")
        col.db.execute("VACUUM ANALYZE")
    }

    /**
     * Track database corruption problems and post analytics events for tracking
     *
     * @param integrityCheckProblems list of problems, the first 10 will be used
     */
    private fun logProblems(integrityCheckProblems: ArrayList<String>) {
        if (integrityCheckProblems.size > 0) {
            val additionalInfo = StringBuffer()
            var i = 0
            while (i < 10 && integrityCheckProblems.size > i) {
                additionalInfo.append(integrityCheckProblems[i]).append("\n")
                // log analytics event so we can see trends if user allows it
                UsageAnalytics.sendAnalyticsEvent("DatabaseCorruption", integrityCheckProblems[i])
                i++
            }
            Timber.i("fixIntegrity() Problem list (limited to first 10):\n%s", additionalInfo)
        } else {
            Timber.i("fixIntegrity() no problems found")
        }
    }
}
