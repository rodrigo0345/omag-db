# omag-db

# Architectural objectives

- Row based structured
- On-Disk database
- With on-memory cache manager

## Access Methods

### Page Layouts

- The size of the page depends on the system used 
- Little Indian Byte Organization (LSB)
- Slotted Pages to store variable size records
- Logical sorting
- Magic numbers are used in a meta table to find the version of the file format used
- Checksum verification (to implement - add to page header)
- Each record needs to introduce a column (omag_check) to make sure we can recover from checksum verification error
- Right most neighbour is stored in the header

Header

[Cell] -> (KeyVal, KeyLen)
[Content] -> (Value)

### B+Tree Design

- Merge empty pages (internal | leaf)
- Use Overflow pages
- Use breadcums to keep track of the path done (bcstack)
- Pluggable compression method (either row-wise or page-wise)

