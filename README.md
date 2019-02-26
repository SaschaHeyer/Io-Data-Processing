# Large data processing with Cloud Dataflow and Cloud Datastore

In one of my projects, I had the requirement to process large text files of the size of hundreds of MB up to GB or even TB. A user uploads a csv file and we are required to write each row into Google Cloud Datastore (no-SQL document database).
Obviously, in this size ranges it is not archivable using a simple web server. That's where I took advantage of Google Cloud Dataflow, wrote a pipeline to process these csv files and saved them to the Google Cloud Datastore. In this article, I want to share with you how I solved this task.

See the [article]() for details.
