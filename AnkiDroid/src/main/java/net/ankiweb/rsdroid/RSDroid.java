package net.ankiweb.rsdroid;

import org.json.JSONException;
import org.json.JSONObject;

public class RSDroid {
    static {
        System.loadLibrary("rsdroid");
    }

    private static native String request(final String command, final String args);

    // returns (daysElapsed, nextDayAt)
    public static int[] timingToday(int createdSecs, int createdMinsWest, int rolloverHour) {
        try {
            JSONObject i = new JSONObject();
            i.put("created_secs", createdSecs);
            i.put("created_mins_west", createdMinsWest);
            i.put("rollover_hour", rolloverHour);

            String output = request("timingToday", i.toString());

            JSONObject obj = new JSONObject(output);
            int[] o = new int[2];
            o[0] = obj.getInt("days_elapsed");
            o[1] = obj.getInt("next_day_at");
            return o;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
}
