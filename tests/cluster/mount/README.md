This entire folder and its functions are deprecated.
Mounting storage accounts is not the recommended way to access them.
The code is kept here to allow it to be reactivated if needed.

The tests of the class `EventHubCapture` requires mountpoints, or so it seems.
The external avro SerDe classes do not seem to support direct access. Those tests 
are skipped together with the deprecation of the class itself. If those tests are 
removed entirely, this folder should probably disappear, too.
