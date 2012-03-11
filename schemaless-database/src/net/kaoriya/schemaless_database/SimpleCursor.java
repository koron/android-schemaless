package net.kaoriya.schemaless_database;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

final class SimpleCursor implements SchemalessCursor
{

    private final int indexId;

    private final int indexUtime;

    private SQLiteDatabase database;

    private Cursor cursor;

    SimpleCursor(SQLiteDatabase database, Cursor cursor)
    {
        this.indexId = cursor.getColumnIndexOrThrow(
                SchemalessDatabase.COLUMN_ID);
        this.indexUtime = cursor.getColumnIndexOrThrow(
                SchemalessDatabase.COLUMN_UTIME);
        this.database = database;
        this.cursor = cursor;
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            _close();
        }
    }

    @Override
    public boolean moveToNext()
    {
        return this.cursor.moveToNext();
    }

    @Override
    public long getId()
    {
        return cursor.getLong(this.indexId);
    }

    @Override
    public long getUtime()
    {
        return cursor.getLong(this.indexUtime);
    }

    @Override
    public ContentValues get()
    {
        return SchemalessDatabase.getRecord(this.database, getId());
    }

    @Override
    public void close()
    {
        _close();
    }

    private synchronized void _close()
    {
        if (this.cursor != null)
        {
            this.cursor.close();
            this.cursor = null;
        }

        // Don't close database for efficiency reason.
    }

}
