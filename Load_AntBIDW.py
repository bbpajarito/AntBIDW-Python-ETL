import pyodbc
import pygrametl
import ConnectionStrings as CS
import SourceSQLQueries as SSQ
from datetime import datetime

from pygrametl.datasources import TypedCSVSource, SQLSource
from pygrametl.tables import Dimension, TypeOneSlowlyChangingDimension, FactTable, AccumulatingSnapshotFactTable

# Open a connection to the OLTP AntBil
AntBilReplication_conn = pyodbc.connect(CS.AntBilReplication_string)

# Open a connection to the DW AntBil and create a ConnectionWrapper
AntBilDW_conn = pyodbc.connect(CS.AntBilDW_string)
AntBilDW_conn_wrapper = pygrametl.ConnectionWrapper(AntBilDW_conn)
AntBilDW_conn_wrapper.setasdefault()

# Create the data source of each dimension table
attribute_mapping = {'DateKey' : int, 'DayOfWeek' : int, 'DayOfMonth' : int, 'DayOfYear' : int, 'WeekOfYear' : int, \
                    'MonthOfYear' : int, 'CalendarQuarter' : int, 'CalendarYear' : int, 'FiscalMonthOfYear' : int, \
                    'FiscalQuarter' : int, 'FiscalYear' : int}
DimDate_source = TypedCSVSource(f=open('DimDate_2017-2037.csv', 'r', 16384), casts=attribute_mapping, delimiter=',')
DimGroup_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.DimGroup_query)
DimGroupCategory_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.DimGroupCategory_query)
DimRole_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.DimRole_query)
DimCandidate_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.DimCandidate_query)
DimMeetingType_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.DimMeetingType_query)
DimBadge_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.DimBadge_query)
DimLocation_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.DimLocation_query)
DimLevel_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.DimLevel_query)

# Create the data source of each fact table
Fact_CandidateGroup_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_CandidateGroup_query)
Fact_CandidateMeetingAttendance_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_CandidateMeetingAttendance_query)
Fact_CandidateRecognition_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_CandidateRecognition_query)
Fact_CandidateReview_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_CandidateReview_query)
Fact_CandidateWorkCommittedPattern_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_CandidateWorkCommittedPattern_query)
Fact_MeetingAttendanceCommittedPattern_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_MeetingAttendanceCommittedPattern_query)
Fact_WorkDays_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_WorkDays_query)
Fact_WorkAttendance_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_WorkAttendance_query)
Fact_CandidateConversation_source = SQLSource(connection=AntBilReplication_conn, query=SSQ.Fact_CandidateConversation_query)

# Methods
def convert_FullDate(row):
    # Convert FullDate to datetime
    return datetime.strptime(row['FullDate'], '%d/%m/%Y')

def key_finder(row, namemapping):
    # Assign the primary key of DimDate
    return row[namemapping['FullDate']]

# Create dimension and fact table abstractions for use in the ETL flow
DimDate = Dimension(
    name='DimDate',
    key='DateKey',
    attributes=['FullDate', 'DateName', 'DayOfWeek', 'DayNameOfWeek', 'DayOfMonth', 'DayOfYear', 'WeekdayWeekend', \
                'WeekOfYear', 'MonthName', 'MonthOfYear', 'IsLastDayOfMonth', 'CalendarQuarter', 'CalendarYear', \
                'CalendarYearMonth', 'CalendarYearQtr', 'FiscalMonthOfYear', 'FiscalQuarter', 'FiscalYear', \
                'FiscalYearMonth', 'FiscalYearQtr'],
    lookupatts=['FullDate'],
    idfinder=key_finder
)

DimGroup = TypeOneSlowlyChangingDimension(
    name='DimGroup',
    key='GroupKey',
    attributes=['GroupId', 'GroupName', 'Version'],
    lookupatts=['GroupId']
)

DimGroupCategory = Dimension(
    name='DimGroupCategory',
    key='GroupCategoryKey', 
    attributes=['GroupCategoryId', 'GroupCategoryName'],
    lookupatts=['GroupCategoryId']
)

DimRole = Dimension(
    name='DimRole',
    key='RoleKey', 
    attributes=['RoleId', 'Role'],
    lookupatts=['RoleId']
)

DimCandidate = TypeOneSlowlyChangingDimension(
    name='DimCandidate',
    key='CandidateKey',
    attributes=['CandidateId', 'FirstName', 'MiddleName', 'LastName', 'LinkedInProfile', 'CurrentRole', 'Version'],
    lookupatts=['CandidateId']
)

DimMeetingType = Dimension(
    name='DimMeetingType',
    key='MeetingTypeKey',
    attributes=['MeetingTypeId', 'MeetingType'],
    lookupatts=['MeetingTypeId']
)

DimBadge = TypeOneSlowlyChangingDimension(
    name='DimBadge',
    key='BadgeKey',
    attributes=['BadgeId', 'Badge', 'Version'],
    lookupatts=['BadgeId']
)

DimLocation = TypeOneSlowlyChangingDimension(
    name='DimLocation',
    key='LocationKey',
    attributes=['LocationId', 'Location'],
    lookupatts=['LocationId']
)

DimLevel = Dimension(
    name='DimLevel',
    key='LevelNameKey',
    attributes=['LevelNameId', 'LevelName'],
    lookupatts=['LevelName']
)

Fact_CandidateGroup = AccumulatingSnapshotFactTable(
    name='Fact_CandidateGroup',
    keyrefs=['CandidateKey', 'GroupKey', 'GroupCategoryKey', 'RoleKey', 'StartDateKey', 'ExpiryDateKey'],
    otherrefs=['Version'],
    measures=[]
)

Fact_CandidateMeetingAttendance = AccumulatingSnapshotFactTable(
    name='Fact_CandidateMeetingAttendance',
    keyrefs=['CandidateKey', 'MeetingTypeKey', 'MeetingDateKey'],
    otherrefs=['Version'],
    measures=['TotalMeeting']
)

Fact_CandidateRecognition = AccumulatingSnapshotFactTable(
    name='Fact_CandidateRecognition',
    keyrefs=['CandidateKey', 'BadgeKey', 'FirstObtainedDateKey'],
    otherrefs=['Version'],
    measures=['TotalBadges']
)

Fact_CandidateReview = AccumulatingSnapshotFactTable(
    name='Fact_CandidateReview',
    keyrefs=['CandidateKey', 'LocationKey', 'LevelNameKey', 'CreatedOnDateKey', 'MenteeReviewId', 'MentorReviewId', \
        'LatestAnswerId'],
    otherrefs=['Strengths', 'Weaknessess', 'MentorReview', 'Version'],
    measures=['Points', 'PointsQH', 'Level', 'ProfessionalismRating', 'CommunicationRating', 'InitiativeRating', \
        'ReliabilityRating', 'TechnicalSkillRating', 'CriticalThinkingRating', 'QualityOfWorkRating', 'TeamworkRating', \
        'OverallRating', 'MentorRating', 'TechnicalAssessmentCorrect', 'TechnicalAssessmentIncorrect'],
)

Fact_CandidateWorkCommittedPattern = AccumulatingSnapshotFactTable(
    name='Fact_CandidateWorkCommittedPattern',
    keyrefs=['CandidateKey', 'StartDateKey', 'EndDateKey'],
    otherrefs=['Version'],
    measures=['NumberOfHourPerWeek', 'NumberOfDayPerWeek', 'TotalHour']
)

Fact_MeetingAttendanceCommittedPattern = AccumulatingSnapshotFactTable(
    name='Fact_MeetingAttendanceCommittedPattern',
    keyrefs=['CandidateKey', 'StartDateKey', 'ExpiryDateKey', 'MeetingTypeKey'],
    otherrefs=['Version'],
    measures=['NumberOfMeetingsPerWeek', 'TotalNumberOfMeetings']
)

Fact_WorkDays = AccumulatingSnapshotFactTable(
    name='Fact_WorkDays',
    keyrefs=['CandidateKey', 'StartDateKey', 'LatestWorkDateKey'],
    otherrefs=['Version'],
    measures=['TotalWorkingDays', 'DaysPresent', 'DaysAbsent']
)

Fact_WorkAttendance = AccumulatingSnapshotFactTable(
    name='Fact_WorkAttendance',
    keyrefs=['CandidateKey', 'GroupKey', 'DateKey'],
    otherrefs=['Version', 'TimeIn', 'TimeOut'],
    measures=['TotalHoursWorked']
)

Fact_CandidateConversation = AccumulatingSnapshotFactTable(
    name='Fact_CandidateConversation',
    keyrefs=['CandidateKey', 'ConversationDateKey', 'ConversationRole'],
    otherrefs=['Version'],
    measures=['Rating']
)

# Load the dimension tables
for row in DimDate_source:
    row['FullDate'] = convert_FullDate(row)
    DimDate.ensure(row, namemapping={'DateKey':'DateKey'})

for row in DimGroup_source:
    row['Version'] = CS.VersionNumber
    DimGroup.scdensure(row)

for row in DimGroupCategory_source:
    DimGroupCategory.ensure(row)

for row in DimRole_source:
    DimRole.ensure(row)

for row in DimCandidate_source:
    row['Version'] = CS.VersionNumber
    DimCandidate.scdensure(row)

for row in DimMeetingType_source:
    DimMeetingType.ensure(row)

for row in DimBadge_source:
    row['Version'] = CS.VersionNumber
    DimBadge.scdensure(row)

for row in DimLocation_source:
    DimLocation.scdensure(row)

for row in DimLevel_source:
    DimLevel.ensure(row)

# Load the fact tables
for row in Fact_CandidateGroup_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['GroupKey'] = DimGroup.lookup(row)
    row['GroupCategoryKey'] = DimGroupCategory.lookup(row)
    row['RoleKey'] = DimRole.lookup(row)
    row['StartDate'] = row['StartDate'].date()
    row['ExpiryDate'] = row['ExpiryDate'].date() 
    row['StartDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'StartDate'})
    row['ExpiryDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'ExpiryDate'})
    row['Version'] = CS.VersionNumber
    Fact_CandidateGroup.ensure(row)

for row in Fact_CandidateMeetingAttendance_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['MeetingTypeKey'] = DimMeetingType.lookup(row)
    row['MeetingDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'MeetingDate'})
    row['Version'] = CS.VersionNumber
    Fact_CandidateMeetingAttendance.ensure(row)

for row in Fact_CandidateRecognition_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['BadgeKey'] = DimBadge.lookup(row)
    row['FirstObtainedDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'FirstObtainedOn'})
    row['Version'] = CS.VersionNumber
    Fact_CandidateRecognition.ensure(row)

for row in Fact_CandidateReview_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['LocationKey'] = DimLocation.lookup(row)
    row['LevelNameKey'] = DimLevel.lookup(row)
    row['CreatedOnDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'CreatedOn'})
    row['Version'] = CS.VersionNumber
    Fact_CandidateReview.ensure(row)

for row in Fact_CandidateWorkCommittedPattern_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['StartDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'StartDate'})
    row['EndDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'EndDate'})
    row['Version'] = CS.VersionNumber
    Fact_CandidateWorkCommittedPattern.ensure(row)

for row in Fact_MeetingAttendanceCommittedPattern_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['StartDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'StartDate'})
    row['ExpiryDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'ExpiryDate'})
    row['MeetingTypeKey'] = DimMeetingType.lookup(row)
    row['Version'] = CS.VersionNumber
    Fact_MeetingAttendanceCommittedPattern.ensure(row)

for row in Fact_WorkDays_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['StartDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'StartDate'})
    row['LatestWorkDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'LatestWorkDate'})
    row['Version'] = CS.VersionNumber
    Fact_WorkDays.ensure(row)

for row in Fact_WorkAttendance_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['GroupKey'] = DimGroup.lookup(row)
    row['DateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'Date'})
    row['Version'] = CS.VersionNumber
    Fact_WorkAttendance.ensure(row)

for row in Fact_CandidateConversation_source:
    row['CandidateKey'] = DimCandidate.lookup(row)
    row['ConversationDateKey'] = DimDate.lookup(row, namemapping={'FullDate' : 'ConversationDate'})
    row['Version'] = CS.VersionNumber
    Fact_CandidateConversation.ensure(row)

    # if not row['CandidateKey']:
    #     print(row)
    #     raise ValueError("CandidateId is not present in the DimCandidate")

# Commit changes and close the connections
AntBilDW_conn_wrapper.commit()
AntBilDW_conn_wrapper.close()

AntBilReplication_conn.close()