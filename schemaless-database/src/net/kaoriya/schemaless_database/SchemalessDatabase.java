package net.kaoriya.schemaless_database;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public final class SchemalessDatabase implements Schemaless {

    public final static int DB_VERSION = 1;

    private SQLiteOpenHelper openHelper;

    SchemalessDatabase(Context context, String name)
    {
        this.openHelper = new SQLiteOpenHelper(context, name, null,
                DB_VERSION)
        {
            @Override
            public void onCreate(SQLiteDatabase db) {
                setupSchema(db);
            }

            @Override
            public void onUpgrade(SQLiteDatabase db, int oldVer, int newVer) {
                // Implement in future.
            }
        };
    }

    /**
     * Setup schema.
     */
    private static void setupSchema(SQLiteDatabase db) {
        db.execSQL(
                "CREATE TABLE field (" +
                "  _id INTEGER PRIMARY KEY," +
                "  name STRING NOT NULL UNIQUE" +
                ")");
        db.execSQL(
                "CREATE TABLE rechdr (" +
                "  _id INTEGER PRIMARY KEY," +
                "  utime INTEGER NOT NULL DEFAULT (datetime('now'))" +
                ")");
        db.execSQL(
                "CREATE TABLE propval (" +
                "  _id INTEGER PRIMARY KEY," +
                "  rec_id INTEGER NOT NULL," +
                "  fld_id INTEGER NOT NULL," +
                "  utime INTEGER NOT NULL DEFAULT (datetime('now'))," +
                "  val_type INTEGER NOT NULL DEFAULT 0," +
                "  int_val INTEGER DEFAULT NULL," +
                "  text_val TEXT DEFAULT NULL," +
                "  real_val REAL DEFAULT NULL," +
                "  UNIQUE (rec_id, fld_id)" +
                ")");
        db.execSQL(
                "CREATE INDEX propvals_rec_id__index" +
                "  ON propvals (rec_id)");
        db.execSQL(
                "CREATE INDEX propvals_fld_id__index" +
                "  ON propvals (fld_id)");
    }

    public void close() {
        this.openHelper.close();
    }

}
