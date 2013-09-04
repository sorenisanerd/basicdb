#######
BasicDB
#######

When BasicDB grows up, it wants to be a higly available, flexible,
non-relational data store.

It offers an API that is somewhat compatible with AWS SimpleDB, so if you know
how to deal with SimpleDB, you should know how to deal with BasicDB.

The following API calls work (sufficiently to make the positive tests pass):

 * CreateDomain
 * DeleteAttributes
 * DeleteDomain
 * DomainMetadata
 * GetAttributes
 * ListDomains
 * PutAttributes
 * Select
 * BatchPutAttributes
 * BatchDeleteAttributes

Things BasicDB doesn't do:
 * Authentication
