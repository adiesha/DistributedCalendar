# DistributedCalendar

A distributed calendar application using a replicated log and dictionary


4 points - Log and dictionary implementation
2 - log is truncated correctly
2 - nodes use permanent storage


8 points - Calendar implementation
1 - can check if calendar free (show calendar)
1 - can schedule event with itself
2 - replicates event to other nodes' logs
1 - schedule with others
2 - can delete appointments in the distributed log
1 - deleted events removed from calendar


5 points - Calendar event conflict resolution
2 - scheduling conflicts detected
2 - scheduling conflict resolution algorithm correct
1 - scheduling resolution implemented