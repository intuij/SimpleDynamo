package edu.buffalo.cse.cse486586.simpledynamo;

import android.app.Activity;
import android.content.ContentValues;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);

		final Uri URI = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

		Button insert = (Button) findViewById(R.id.button1);
		insert.setOnClickListener(new View.OnClickListener(){
			@Override
			public void onClick(View v) {
				ContentValues cv = new ContentValues();
				cv.put("key", "FygQdysGFc8YOitrDsCZtU7jMDj9k2yf");
				cv.put("value", "gdgfd28gydbgwehg3t783gr3");
				getContentResolver().insert(URI, cv);
			}
		});

		Button queryall = (Button) findViewById(R.id.button2);
		queryall.setOnClickListener(new View.OnClickListener() {
			@Override
			public void onClick(View v) {
				Cursor result = getContentResolver().query(URI, null, "FygQdysGFc8YOitrDsCZtU7jMDj9k2yf", null, null);
				if (result != null) {
					result.moveToFirst();
					while (!result.isAfterLast()) {
						Log.d("Query", result.getString(0) + "," + result.getString(1));
						result.moveToNext();
					}
				}
				result.close();
			}
		});
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	public void onStop() {
        super.onStop();
	    Log.v("Test", "onStop()");
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

}
