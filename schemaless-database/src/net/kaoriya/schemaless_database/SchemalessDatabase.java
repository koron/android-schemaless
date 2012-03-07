package net.kaoriya.schemaless_database;

import java.util.Map;

import android.content.ContentValues;
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

    public void close() {
        this.openHelper.close();
    }

    ////////////////////////////////////////////////////////////////////////

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

    private static long insertRecord(SQLiteDatabase db, ContentValues cv)
    {
        db.beginTransaction();
        try {
            long now = System.currentTimeMillis();
            long recId = insertRechdr(db, now);
            if (recId > 0) {
                insertPropval(db, recId, cv, now);
            }
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
        return 0;
    }

    private static boolean updateRecord(
            SQLiteDatabase db,
            long recId,
            ContentValues cv)
    {
        db.beginTransaction();
        try {
            long now = System.currentTimeMillis();
            if (updateRechdr(db, recId, now)) {
                updatePropval(db, recId, cv, now);
                db.setTransactionSuccessful();
            }
        } finally {
            db.endTransaction();
        }
        return false;
    }

    private static void deleteRecord(SQLiteDatabase db, long recId)
    {
        db.beginTransaction();
        try {
            deleteRechdr(db, recId);
            deletePropval(db, recId);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    private static ContentValues getRecord(SQLiteDatabase db, long recId)
    {
        // TODO:
        return null;
    }

    private static long getField(SQLiteDatabase db, String name)
    {
        // TODO:
        return 0;
    }

    private static long assureField(SQLiteDatabase db, String name)
    {
        // TODO:
        return 0;
    }

    private static long insertRechdr(SQLiteDatabase db, long time)
    {
        // TODO:
        return 0;
    }

    private static boolean updateRechdr(
            SQLiteDatabase db,
            long recId,
            long time)
    {
        // TODO:
        return false;
    }

    private static void deleteRechdr(SQLiteDatabase db, long recId)
    {
        // TODO:
    }

    private static void insertPropval(
            SQLiteDatabase db,
            long recId,
            ContentValues cv,
            long now)
    {
        for (Map.Entry<String,Object> value : cv.valueSet()) {
            String k = value.getKey();
            Object v = value.getValue();
            if (v != null) {
                assurePropval(db, recId, assureField(db, k), v, now);
            }
        }
    }

    private static void updatePropval(
            SQLiteDatabase db,
            long recId,
            ContentValues cv,
            long now)
    {
        for (Map.Entry<String,Object> value : cv.valueSet()) {
            String k = value.getKey();
            Object v = value.getValue();
            if (v != null) {
                assurePropval(db, recId, assureField(db, k), v, now);
            } else {
                long fieldId = getField(db, k);
                if (fieldId > 0) {
                    deletePropval(db, recId, fieldId);
                }
            }
        }
    }

    private static void assurePropval(
            SQLiteDatabase db,
            long recId,
            long fieldId,
            Object value,
            long now)
    {
        // TODO:
    }

    private static void deletePropval(SQLiteDatabase db, long recId)
    {
        // TODO:
        // should be implemented by trigger?
    }

    private static void deletePropval(
            SQLiteDatabase db,
            long recId,
            long fieldId)
    {
        // TODO:
    }


}
