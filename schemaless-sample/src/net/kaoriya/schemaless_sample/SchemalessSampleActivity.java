package net.kaoriya.schemaless_sample;

import android.app.Activity;
import android.os.Bundle;

import net.kaoriya.schemaless_database.SchemalessDatabase;
import net.kaoriya.schemaless_database.SchemalessFactory;

public class SchemalessSampleActivity extends Activity
{
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
    }

}
