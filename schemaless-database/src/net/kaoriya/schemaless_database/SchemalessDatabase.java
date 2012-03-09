package net.kaoriya.schemaless_database;

import java.util.Map;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
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

    public final static String TABLE_FIELD = "field";
    public final static String TABLE_RECHDR = "rechdr";
    public final static String TABLE_PROPVAL = "propval";

    public final static String COLUMN_ID = "_id";

    public final static int VALTYPE_BOOL        = 0;
    public final static int VALTYPE_BYTE        = 1;
    public final static int VALTYPE_SHORT       = 2;
    public final static int VALTYPE_INT         = 3;
    public final static int VALTYPE_LONG        = 4;
    public final static int VALTYPE_STRING      = 5;
    public final static int VALTYPE_BYTEARRAY   = 6;
    public final static int VALTYPE_FLOAT       = 7;
    public final static int VALTYPE_DOUBLE      = 8;

    /**
     * Setup schema.
     */
    private static void setupSchema(SQLiteDatabase db) {
        db.execSQL(
                "CREATE TABLE " + TABLE_FIELD + " (" +
                "  _id INTEGER PRIMARY KEY," +
                "  name STRING NOT NULL UNIQUE" +
                ")");
        db.execSQL(
                "CREATE TABLE " + TABLE_RECHDR + " (" +
                "  _id INTEGER PRIMARY KEY," +
                "  utime INTEGER NOT NULL DEFAULT (datetime('now'))" +
                ")");
        db.execSQL(
                "CREATE TABLE " + TABLE_PROPVAL + " (" +
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
                "CREATE INDEX propval_rec_id__index" +
                "  ON propval (rec_id)");
        db.execSQL(
                "CREATE INDEX propval_fld_id__index" +
                "  ON propval (fld_id)");
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
        Long utime = getRechdr(db, recId);
        if (utime == null) {
            return null;
        } else {
            return getPropvals(db, recId, new ContentValues());
        }
    }

    private static long getField(SQLiteDatabase db, String name)
    {
        // TODO:
        return 0;
    }

    private final static String[] COLUMNS_GET_FIELD_NAME = { "name" };

    private static String getFieldName(SQLiteDatabase db, long fieldId)
    {
        Cursor c = db.query(TABLE_FIELD, COLUMNS_GET_FIELD_NAME, "_id=?",
                new String[] { Long.toString(fieldId) },
                null, null, null, "1");
        try {
            String retval = null;
            if (c.moveToNext()) {
                retval = c.getString(0);
            }
            return retval;
        } finally {
            c.close();
        }
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

    private final static String[] COLUMNS_GET_RECHDR = { "utime" };

    private static Long getRechdr(SQLiteDatabase db, long recId)
    {
        Cursor c = db.query(TABLE_RECHDR, COLUMNS_GET_RECHDR, "_id=?",
                new String[] { Long.toString(recId) }, null, null, null, "1");
        try {
            Long retval = null;
            if (c.moveToNext()) {
                retval = Long.valueOf(c.getLong(0));
            }
            return retval;
        } finally {
            c.close();
        }
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

    private final static String[] COLUMNS_GET_PROPVAL = {
        "fld_id",
        "val_type",
        "int_val",
        "text_val",
        "real_val"
    };

    private static ContentValues getPropvals(
            SQLiteDatabase db,
            long recId,
            ContentValues cv)
    {
        Cursor c = db.query(TABLE_PROPVAL, COLUMNS_GET_PROPVAL, "rec_id=?",
                new String[] { Long.toString(recId) }, null, null, null, null);
        try {
            cv.put(COLUMN_ID, recId);
            while (c.moveToNext()) {
                getPropval(db, c, cv);
            }
        } finally {
            c.close();
        }
        return cv;
    }

    private static void getPropval(
            SQLiteDatabase db,
            Cursor cursor,
            ContentValues cv)
    {
        String name = getFieldName(db, cursor.getLong(0));
        if (name == null) {
            // FIXME: unknown field error.
            return;
        }

        switch (cursor.getInt(1)) {
        case VALTYPE_BOOL:
            cv.put(name, (cursor.getInt(2) != 0 ? true : false));
            break;
        case VALTYPE_BYTE:
            cv.put(name, (byte)cursor.getInt(2));
            break;
        case VALTYPE_SHORT:
            cv.put(name, cursor.getShort(2));
            break;
        case VALTYPE_INT:
            cv.put(name, cursor.getInt(2));
            break;
        case VALTYPE_LONG:
            cv.put(name, cursor.getLong(2));
            break;
        case VALTYPE_STRING:
            cv.put(name, cursor.getString(3));
            break;
        case VALTYPE_BYTEARRAY:
            cv.put(name, cursor.getBlob(3));
            break;
        case VALTYPE_FLOAT:
            cv.put(name, cursor.getFloat(4));
            break;
        case VALTYPE_DOUBLE:
            cv.put(name, cursor.getDouble(4));
            break;
        default:
            // FIXME: unknown valtype error.
            break;
        }

        return;
    }

}
