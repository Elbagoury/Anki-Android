package net.ankiweb.rsdroid;

public class RSDroid {
    static {
        System.loadLibrary("rsdroid");
    }

    private static native String request(final String input);

    public static String runCommand(String to) {
        return request(to);
    }
}
