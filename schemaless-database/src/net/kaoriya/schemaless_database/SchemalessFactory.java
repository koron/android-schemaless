package net.kaoriya.schemaless_database;

import android.content.Context;

public final class SchemalessFactory implements Schemaless {

    private Context context;
    private String name;

    public SchemalessFactory(Context context, String name) {
        this.context = context;
        this.name = name;
    }

    public SchemalessDatabase newDatabase() {
        return null;
    }

}
