# Lens Warehouse ODM

## Compatibility

### No MetaDataVersion

Lens Warehouse omits the MetaDataVersion and so provides only one current 
version. Importers should create a warning if a ODM file with more than one
MetaDataVersion is about to be imported.

### No OrderNumber

In ODM the elements StudyEventRef, FormRef, ItemGroupRef and ItemRef have an
OrderNumber. The OrderNumbers provide an ordering of the reference targets 
(StudyEventDef, FormDef, ...) for use whenever a list of such targets is
presented to a user. They do not imply anything about event scheduling, time 
ordering, or data correctness.

Lens Warehouse doesn't support OrderNumbers and instead uses the Name of the
reference targets to provide an ordering for user facing lists.
