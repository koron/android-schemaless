package net.kaoriya.schemaless_sample;

import java.util.Map;

import android.app.ListActivity;
import android.content.ContentValues;
import android.os.Bundle;
import android.util.Log;
import android.view.View;

import net.kaoriya.schemaless_database.SchemalessDatabase;
import net.kaoriya.schemaless_database.SchemalessFactory;

public class SchemalessSampleActivity
    extends ListActivity
    implements View.OnClickListener
{
    public final static String TAG = "SCHEMALESS";

    private SchemalessDatabase database;

    /** Called when the activity is first created. */
    @Override
    public void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.main);

        SchemalessFactory factory = new SchemalessFactory(this,
                "schemaless.db");
        this.database = factory.newDatabase();

        findViewById(R.id.DumpCV).setOnClickListener(this);
        findViewById(R.id.AddRecord).setOnClickListener(this);
        findViewById(R.id.RemoveRecord).setOnClickListener(this);
    }

    @Override
    protected void onDestroy()
    {
        super.onDestroy();
        this.database.close();
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
        case R.id.DumpCV:
            dumpContentValues();
            break;
        case R.id.AddRecord:
            addRecord();
            break;
        case R.id.RemoveRecord:
            removeRecord();
            break;
        }
    }

    private void dumpContentValues() {
        Log.v(TAG, "#dumpContentValues");
        ContentValues cv = new ContentValues();

        cv.put("byte", Byte.valueOf((byte)0x12));
        cv.put("int", Integer.valueOf(1234));
        cv.put("float", Float.valueOf(12.34f));
        cv.put("short", Short.valueOf((short)1234));
        cv.put("byte_array", new byte[] { 0, 1, 2, 3, 4 });
        cv.put("string", "foobar");
        cv.put("double", Double.valueOf(1.23456));
        cv.put("long", Long.valueOf(123456789));
        cv.put("bool", Boolean.TRUE);

        int index = 0;
        for (Map.Entry<String,Object> value : cv.valueSet()) {
            String k = value.getKey();
            Object v = value.getValue();
            String type = "(null)";
            if (v != null) {
                type = v.getClass().getName();
            }
            Log.v(TAG, String.format("  #%1$d \"%2$s\" %3$s", index, k, type));
        }
    }

    private void addRecord() {
        Log.v(TAG, "#addRecord");

        // TODO:

        /*
        ContentValues cv = new ContentValues();
        long id = database.insert(cv);
        Log.v(TAG, "  insert()=" + id);
        */
    }

    private void removeRecord() {
        Log.v(TAG, "#removeRecord");
        // TODO:
    }

}
