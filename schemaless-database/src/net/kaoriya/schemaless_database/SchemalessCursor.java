package net.kaoriya.schemaless_database;

import android.content.ContentValues;

public interface SchemalessCursor {

    boolean moveToNext();

    long getId();

    long getUtime();

    ContentValues get();

    void close();

}
