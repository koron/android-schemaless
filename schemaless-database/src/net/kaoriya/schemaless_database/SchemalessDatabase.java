package net.kaoriya.schemaless_database;

import java.util.Map;
import java.util.HashMap;

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

    public long insertRecord(ContentValues cv)
    {
        SQLiteDatabase db = getWritableDatabase();
        db.beginTransaction();
        try {
            long recId = insertRecord(db, cv);
            db.setTransactionSuccessful();
            return recId;
        } finally {
            db.endTransaction();
        }
    }

    public boolean updateRecord(long recId, ContentValues cv)
    {
        SQLiteDatabase db = getWritableDatabase();
        db.beginTransaction();
        try {
            boolean retval = updateRecord(db, recId, cv);
            db.setTransactionSuccessful();
            return retval;
        } finally {
            db.endTransaction();
        }
    }

    public void deleteRecord(long recId)
    {
        SQLiteDatabase db = getWritableDatabase();
        db.beginTransaction();
        try {
            deleteRecord(db, recId);
            db.setTransactionSuccessful();
            return;
        } finally {
            db.endTransaction();
        }
    }

    public ContentValues selectRecord(long recId)
    {
        SQLiteDatabase db = getReadableDatabase();
        return getRecord(db, recId);
    }

    public void close() {
        this.openHelper.close();
    }

    ////////////////////////////////////////////////////////////////////////

    private SQLiteDatabase getReadableDatabase()
    {
        return this.openHelper.getReadableDatabase();
    }

    private SQLiteDatabase getWritableDatabase()
    {
        return this.openHelper.getWritableDatabase();
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

    public final static Map<Class,Integer> VALUE_TYPEMAP;

    static {
        VALUE_TYPEMAP = new HashMap<Class,Integer>();
        VALUE_TYPEMAP.put(Boolean.class, VALTYPE_BOOL);
        VALUE_TYPEMAP.put(Byte.class, VALTYPE_BYTE);
        VALUE_TYPEMAP.put(Short.class, VALTYPE_SHORT);
        VALUE_TYPEMAP.put(Integer.class, VALTYPE_INT);
        VALUE_TYPEMAP.put(Long.class, VALTYPE_LONG);
        VALUE_TYPEMAP.put(String.class, VALTYPE_STRING);
        VALUE_TYPEMAP.put(byte[].class, VALTYPE_BYTEARRAY);
        VALUE_TYPEMAP.put(Float.class, VALTYPE_FLOAT);
        VALUE_TYPEMAP.put(Double.class, VALTYPE_DOUBLE);
    }

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
            if (recId >= 0) {
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

    private final static String[] COLUMNS_GET_FIELD = { "_id" };

    private static Long getField(SQLiteDatabase db, String name)
    {
        Cursor c = db.query(TABLE_FIELD, COLUMNS_GET_FIELD, "name=?",
                new String[] { name }, null, null, null, "1");
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

    private static Long assureField(SQLiteDatabase db, String name)
    {
        db.beginTransaction();
        try {
            Long retval = getField(db, name);
            if (retval == null) {
                ContentValues cv = new ContentValues();
                cv.put("name", name);
                long id = db.insert(TABLE_FIELD, null, cv);
                retval = (id >= 0) ? Long.valueOf(id) : null;
            }
            return retval;
        } finally {
            db.endTransaction();
        }
    }

    private static long insertRechdr(SQLiteDatabase db, long time)
    {
        ContentValues cv = new ContentValues();
        cv.put("utime", time);
        return db.insert(TABLE_RECHDR, null, cv);
    }

    private static boolean updateRechdr(
            SQLiteDatabase db,
            long recId,
            long time)
    {
        ContentValues cv = new ContentValues();
        cv.put("utime", time);
        int count = db.update(TABLE_RECHDR, cv, "_id=?",
                new String[] { Long.toString(recId) });
        return (count == 1) ? true : false;
    }

    private static void deleteRechdr(SQLiteDatabase db, long recId)
    {
        db.delete(TABLE_RECHDR, "_id=?",
                new String[] { Long.toString(recId) });
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
                Long fieldId = getField(db, k);
                if (fieldId != null) {
                    deletePropval(db, recId, fieldId.longValue());
                }
            }
        }
    }

    private static void assurePropval(
            SQLiteDatabase db,
            long recId,
            Long fieldId,
            Object value,
            long now)
    {
        if (fieldId == null || value == null) {
            return;
        }

        Integer valueType = VALUE_TYPEMAP.get(value.getClass());
        if (valueType == null) {
            return;
        }

        // Setup content values.
        ContentValues cv = new ContentValues();
        cv.put("rec_id", recId);
        cv.put("fld_id", fieldId);
        cv.put("utime", now);
        cv.put("val_type", valueType);
        cv.putNull("int_val");
        cv.putNull("text_val");
        cv.putNull("real_val");

        // Set value.
        switch (valueType.intValue()) {
        case VALTYPE_BOOL:
            cv.put("int_val", (Boolean)value);
            break;
        case VALTYPE_BYTE:
            cv.put("int_val", (Byte)value);
            break;
        case VALTYPE_SHORT:
            cv.put("int_val", (Short)value);
            break;
        case VALTYPE_INT:
            cv.put("int_val", (Integer)value);
            break;
        case VALTYPE_LONG:
            cv.put("int_val", (Long)value);
            break;
        case VALTYPE_STRING:
            cv.put("text_val", (String)value);
            break;
        case VALTYPE_BYTEARRAY:
            cv.put("text_val", (byte[])value);
            break;
        case VALTYPE_FLOAT:
            cv.put("real_val", (Float)value);
            break;
        case VALTYPE_DOUBLE:
            cv.put("real_val", (Float)value);
            break;
        default:
            // FIXME: unknown valueType error.
            break;
        }

        db.insertWithOnConflict(TABLE_RECHDR, null, cv,
                SQLiteDatabase.CONFLICT_REPLACE);
    }

    private static void deletePropval(SQLiteDatabase db, long recId)
    {
        // should be implemented by trigger?
        db.delete(TABLE_PROPVAL, "rec_id=?",
                new String[] { Long.toString(recId) });
    }

    private static void deletePropval(
            SQLiteDatabase db,
            long recId,
            long fieldId)
    {
        db.delete(TABLE_PROPVAL, "rec_id=? AND fld_id=?",
                new String[] {
                    Long.toString(recId), Long.toString(fieldId)
                });
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
            // FIXME: unknown valueType error.
            break;
        }

        return;
    }

}
