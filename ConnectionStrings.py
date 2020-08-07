# Set a new version number for each load 
VersionNumber = 1

# Change the Database for incremental load 
# AntBilReplication or AntBILReplication_v1
AntBilReplication_string = (
    r'Driver={ODBC Driver 17 for SQL Server};'
    r'Server=LAPTOP-FDS2AFU9;'
    r'Database=AntBilReplication;'
    r'Trusted_Connection=yes;'
)

AntBilDW_string = (
    r'Driver={ODBC Driver 17 for SQL Server};'
    r'Server=LAPTOP-FDS2AFU9;'
    r'Database=AntBilDW;'
    r'Trusted_Connection=yes;'
)



