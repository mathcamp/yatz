# yatz

Package yatz is an ab testing framework for golang appengine apps.
It enables ab testing without making any network requests so its super fast and adds negligible overhead.

This package is split into 2 parts: the code which is called from the app and the query part which is a wrapper over big query.

Yatz runs by logging experiment variants in the appengine logs. In order to make this work you will need to enable streaming of logs to bigquery. This is a setting which can be turned on from your appengine dashboard.

Another thing to note is that it requires you to call yatz.Warmup from your appengine warmup request. If you don't have a warmup request set up, its pretty straightforward to do so in appengine.
Appengine calls the warmup request before it makes any other calls. This is useful for the query to know what variants and experiments currently exist.


## Usage
```
// To get an experiment variant
variant := yatz.Roll(ctx, "userid", "test_experiment") 

// To log an event 
yatz.Fire(ctx, "userid", "opened_activity", 1) 

// The last parameter is the count you want to use for this event. This is used in counting the number of events.
// This becomes useful in scenarios where events have a cardinality associated with them.
yatz.Fire(ctx, "userid", "added_contacts", 5) // Denotes that this user added 5 contacts.

// You can also use it with events which require unique counting
// Eg. you want to track unique invites
yatz.FireWithId(ctx, "userid", "invited", "other_userid")
```

For query usage please see ./query --help.

Note: query requires the "bq" command line utility installed on the computer and set up with a google cloud project.
