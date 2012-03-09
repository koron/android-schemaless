package net.kaoriya.schemaless_sample;

import java.util.Map;

import android.app.Activity;
import android.content.ContentValues;
import android.os.Bundle;
import android.util.Log;
import android.view.View;

import net.kaoriya.schemaless_database.SchemalessDatabase;
import net.kaoriya.schemaless_database.SchemalessFactory;

public class SchemalessSampleActivity
    extends Activity
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
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
        case R.id.DumpCV:
            dumpContentValues();
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

}
