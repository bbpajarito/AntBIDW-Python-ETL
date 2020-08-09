DimGroup_query = '''
    SELECT 
	    Id AS GroupId, 
	    Name AS GroupName,
        IsDeleted
    FROM [dbo].[Group] 
    WHERE IsDeleted = 0;
'''

DimGroupCategory_query = '''
    SELECT 
	    Id AS GroupCategoryId,
	    Name AS GroupCategoryName
    FROM [dbo].[GroupType];
'''

DimRole_query = '''
    SELECT 
	    DISTINCT InternshipRoleId AS RoleId,
	    InternshipRole AS Role  
    FROM ProjectEnrolmentRecord;
'''

DimCandidate_query = '''
    SELECT 
	    U.Id AS CandidateId, 
	    U.FirstName,  
	    IIF(U.MiddleName IS NULL OR U.MiddleName = '', 'Not provided', U.MiddleName) AS MiddleName,
	    IIF(U.LastName IS NULL OR U.LastName = '', 'Not provided', U.LastName) AS LastName,
	    PER.InternshipRole AS CurrentRole,
	    IIF(UP.LinkedInProfile IS NULL OR UP.LinkedInProfile = '', 'Not provided', UP.LinkedInProfile) AS LinkedInProfile
    FROM [dbo].[User] U 
    INNER JOIN ProjectEnrolmentRecord PER ON PER.UserId = U.Id
    LEFT JOIN UserProfile UP ON UP.UserId = U.Id
    WHERE 
	    U.FirstName IS NOT NULL AND U.IsDisabled=0 AND U.IsDeleted=0 AND
	    PER.IsDeleted = 0
'''

DimMeetingType_query = '''
    SELECT
	    Id AS MeetingTypeId,
	    Name AS MeetingType
    FROM MeetingType
'''

DimBadge_query = '''
    SELECT
	    Id AS BadgeId,
	    Name AS Badge,
	    IsDeleted
    FROM Badge
    WHERE IsDeleted = 0
'''

DimLocation_query = '''
    SELECT
	    Id AS LocationId,
	    Name AS Location 
    FROM [dbo].[OfficeLocation] 
'''

DimLevel_query = '''
    WITH CTE AS (
	    SELECT * 
	    FROM (
		    VALUES
			    ('Foot In the Door'),
                ('Good Effort'),
			    ('Well Done'),
			    ('Employable')
            ) AS A (LevelName)
        )
    SELECT
	    ROW_NUMBER() OVER(ORDER BY (SELECT 1)) AS LevelNameId,
	    CTE.LevelName
    FROM CTE
'''

################################################################################################################

Fact_CandidateGroup_query = '''
    SELECT 
	    CandidateId, 
	    GroupId, 
	    GroupName, 
	    GroupCategoryId, 
	    StartDate, 
	    CASE 
		    WHEN GroupCategoryId = 1 THEN ISNULL(ExpiryDate, DATEADD(MONTH, 3, StartDate) )
		    END ExpiryDate, 
	    RoleId
    FROM 
	    (
	    SELECT 
		    U.Id AS CandidateId, 
		    UG.GroupId, 
		    G.Name AS GroupName, 
		    G.GroupTypeId AS GroupCategoryId, 
		    CASE 
			    WHEN G.GroupTypeId = 1 THEN ISNULL(PG.StartDate, PG.CreatedOn)
			    END AS StartDate,
		    PER.ExpiryDate, 
		    PER.InternshipRoleId AS RoleId
	    FROM [dbo].[User] U
	    INNER JOIN [dbo].[UserGroup] UG ON UG.UserId = U.id
	    INNER JOIN [Group] G ON UG.GroupId = G.Id
	    INNER JOIN ProjectEnrolmentRecord PER ON UG.UserId = PER.UserId AND UG.GroupId = PER.GroupId
	    INNER JOIN ProjectGroup PG ON G.Id = PG.GroupId And G.GroupTypeId = 1
	    WHERE 
		    U.IsDisabled = 0 AND U.IsDeleted = 0 AND U.FirstName IS NOT NULL AND 
            UG.IsDeleted = 0 AND 
            G.IsDeleted = 0 AND G.GroupTypeId = 1 AND
		    PER.IsDeleted = 0
	    ) BaseQuery
'''

Fact_CandidateMeetingAttendance_query = '''
    SELECT  
		U.Id AS CandidateId,
		MA.MeetingType AS MeetingTypeId,
		CAST(MA.Date AS date) AS MeetingDate,
		COUNT(*) AS TotalMeeting
	FROM [dbo].[User] U
	INNER JOIN [dbo].[ProjectEnrolmentRecord] PER ON U.Id = PER.UserId
	INNER JOIN [dbo].[ProjectGroup] PG ON PER.GroupId = PG.GroupId
	INNER JOIN [dbo].[MeetingAttendance] MA ON U.Id = MA.UserId
	WHERE 
		U.IsDeleted = 0  and U.IsDisabled = 0 and U.FirstName IS NOT NULL AND
		PER.IsDeleted = 0 
	GROUP BY U.Id, MA.MeetingType, CAST(MA.Date AS date)
'''

Fact_CandidateRecognition_query = '''
    SELECT 
	    UB.UserId AS CandidateId,
	    B.Id AS BadgeId,
	    COUNT (*) AS TotalBadges,
	    MIN(CAST(UB.CreatedOn AS date)) AS FirstObtainedOn
    FROM [dbo].[UserBadges] UB
    INNER JOIN [dbo].[User] U ON U.Id =  UB.UserId
    INNER JOIN [dbo].[Badge] B ON B.Id = UB.BadgeId
    INNER JOIN [dbo].[ProjectEnrolmentRecord] PER ON PER.UserId = U.Id
    WHERE 
	    UB.IsClaimed = 1 AND UB.IsDeleted = 0 AND 
	    U.IsDeleted = 0 AND U.IsDisabled = 0 AND U.FirstName IS NOT NULL AND
	    B.IsDeleted = 0 AND
	    PER.IsDeleted = 0
    GROUP BY UB.UserId, B.Id
    ORDER BY UB.UserId
'''

Fact_CandidateReview_query = '''
    SELECT 
        DISTINCT U.Id AS CandidateId, 
        ISNULL(U.Points, 0.00) AS Points, 
        ISNULL(U.PointsQH, 0.00) AS PointsQH, 
        ISNULL(U.Level, 1.00) AS Level,
        CAST(U.CreatedOn AS date) AS CreatedOn,
        ISNULL(IMR.Id, -1) AS MenteeReviewId,
        IMR.ProfessionalismRating,
        IMR.CommunicationRating,
        IMR.InitiativeRating,
        IMR.ReliabilityRating,
        IMR.TechnicalSkillRating,
        IMR.CriticalThinkingRating,
        IMR.QualityOfWorkRating,
        IMR.TeamworkRating, 
        IMR.OverallRating,
        IIF(IMR.Strengths IS NULL OR IMR.Strengths = '', 'Not provided', IMR.Strengths) AS Strengths,
        IIF(IMR.Weaknessess IS NULL OR IMR.Weaknessess = '', 'Not provided', IMR.Weaknessess) AS Weaknessess,
        ISNULL(IMoR.Id, -1) AS MentorReviewId,
        IMoR.Rating AS MentorRating,
        IIF(IMoR.Review IS NULL OR IMoR.Review = '', 'Not provided', IMoR.Review) AS MentorReview,
        ISNULL(SQ1.LatestAnswerId, -1) AS LatestAnswerId,
        SQ1.AnsweredCorrectCount AS TechnicalAssessmentCorrect,
        SQ1.AnsweredIncorrectCount AS TechnicalAssessmentIncorrect,
        SQ2.LocationId,
        CASE
            WHEN U.Level IS NULL OR U.Level < 2 THEN 'Foot In the Door'
            WHEN U.Level >= 2 AND U.Level < 3 THEN 'Good Effort'
            WHEN U.Level >= 3 AND U.Level < 4 THEN 'Well Done'
            ELSE 'Employable'
        END AS LevelName
    FROM [dbo].[User] U
    INNER JOIN [dbo].[ProjectEnrolmentRecord] PER ON PER.UserId = U.Id
    LEFT JOIN [dbo].[InternshipMenteeReview] IMR ON IMR.UserId = U.Id 
    LEFT JOIN [dbo].[InternshipMentorReview] IMoR ON IMoR.MentorUserId = U.Id
    LEFT JOIN (
	    SELECT
		    UA.UserId AS CandidateId,
		    MAX(UA.Id) AS LatestAnswerId,
		    SUM(CASE UA.AnsweredCorrect WHEN 1 THEN 1 ELSE 0 END) AS AnsweredCorrectCount,
		    SUM(CASE UA.AnsweredCorrect WHEN 0 THEN 1 ELSE 0 END) AS AnsweredIncorrectCount
	    FROM [dbo].[UserAnswer] UA
	    WHERE 
		    UA.IsDeleted = 0
	    GROUP BY UA.UserId
		    ) AS SQ1 ON SQ1.CandidateId = U.Id
    LEFT JOIN (
	    SELECT
		    UG.UserId AS CandidateId,
		    OL.Id AS LocationId
	    FROM [dbo].[UserGroup] UG
	    INNER JOIN [dbo].[OfficeLocation] OL ON OL.Id = UG.LocationId
	    WHERE 
		    UG.IsDeleted = 0 
		    ) AS SQ2 ON SQ2.CandidateId = U.Id
    WHERE 
	    U.IsDisabled = 0 AND U.IsDeleted = 0 AND U.FirstName IS NOT NULL AND
	    PER.IsDeleted = 0 AND
	    IMoR.Review NOT LIKE 'Task has been waiting for review for more than % days'
    ORDER BY CandidateId
'''

Fact_CandidateWorkCommittedPattern_query = '''
    SELECT 
	    CandidateId,
	    StartDate,
	    CAST(ExpiryDate AS date) AS EndDate,
	    NumberOfHourPerWeek,
		NumberOfDayPerWeek,
	    DATEDIFF(WEEK, StartDate, ExpiryDate) * NumberOfHourPerWeek AS TotalHour
    FROM (
	    SELECT 
		    AP.UserId AS CandidateId, 
		    CAST(AP.StartDate AS date) AS StartDate, 
		    CASE 
			    WHEN PER.ExpiryDate IS NULL THEN GETDATE()
			    ELSE PER.ExpiryDate 
		    END AS ExpiryDate,
		    CASE 
			    WHEN AP.NumberOfHourPerWeek = 0 THEN 20
			    ELSE AP.NumberOfHourPerWeek 
		    END AS NumberOfHourPerWeek,
			AP.NumberOfDayPerWeek
	    FROM [dbo].[ProjectEnrolmentRecord] PER 
	    INNER JOIN [dbo].[AttendancePattern] AP ON PER.GroupId = AP.GroupId  AND PER.UserId = AP.UserId 
	    INNER JOIN [dbo].[User] U ON U.Id = PER.UserId
	    WHERE 
		    PER.IsDeleted = 0 AND
		    AP.IsDeleted = 0 AND
		    U.IsDeleted = 0 AND U.IsDisabled = 0 AND U.FirstName IS NOT NULL
		) AS AggregatedAttendancePattern 
    ORDER BY CandidateId
'''

Fact_MeetingAttendanceCommittedPattern_query = '''
	SELECT
		SQ.*,
		DATEDIFF(WEEK, SQ.StartDate, SQ.ExpiryDate) AS NumberOfWeeks,
		SQ.NumberOfMeetingsPerWeek * DATEDIFF(WEEK, SQ.StartDate, SQ.ExpiryDate) AS TotalNumberOfMeetings
	FROM (
		SELECT
			SQ1.CandidateId,
			CAST(SQ1.StartDate AS date) AS StartDate,
			CAST(SQ1.ExpiryDate AS date) AS ExpiryDate,
			MT.Id AS MeetingTypeId,
			CASE
				WHEN MT.Id = 1 THEN 2 
				ELSE 1
			END AS NumberOfMeetingsPerWeek
		FROM MeetingType MT
		FULL JOIN (
				SELECT 
					PER.UserId AS CandidateId,
					CASE 
						WHEN PG.StartDate < '2019-03-20' THEN '2019-03-20'		 
						ELSE PG.StartDate
					END AS StartDate,
					ISNULL(PER.ExpiryDate,DATEADD(MONTH, 3, StartDate) ) AS ExpiryDate
				FROM [dbo].[ProjectEnrolmentRecord] PER
				INNER JOIN [dbo].[ProjectGroup] PG ON PER.GroupId = PG.GroupId
				INNER JOIN [dbo].[User] U ON U.Id = PER.UserId
				WHERE
					PER.IsDeleted = 0 AND
					U.IsDeleted = 0 AND U.IsDisabled = 0 AND U.FirstName IS NOT NULL
				) AS SQ1 ON 1=1
		WHERE 
			MT.Id NOT IN (9, 10, 11, 12)
		) SQ
	ORDER BY CandidateId
'''

Fact_WorkDays_query = '''
	DECLARE @LatestWorkDate date;
	SET @LatestWorkDate = ( SELECT CAST(MAX(A.Date) AS date) AS LatestWorkDate
							FROM [dbo].[Attendance] A )

	SELECT
		SQ.*,
		SQ.TotalWorkingDays - SQ.DaysPresent AS DaysAbsent
	FROM (
		SELECT
			SQ1.CandidateId,
			ISNULL(SQ2.StartDate, CAST('2038-01-01' AS date)) AS StartDate,
			@LatestWorkDate AS LatestWorkDate,
			DATEDIFF(dd, SQ2.StartDate, @LatestWorkDate) AS TotalWorkingDays,
			SQ2.DaysPresent
		FROM (
				SELECT U.Id AS CandidateId
				FROM [dbo].[User] U
				INNER JOIN [dbo].[ProjectEnrolmentRecord] PER ON PER.UserId = U.Id
				WHERE
					U.IsDeleted = 0 AND U.IsDisabled = 0 AND U.FirstName IS NOT NULL AND
					PER.IsDeleted = 0
				) AS SQ1 
		LEFT JOIN (
				SELECT
					A.UserId AS CandidateId,
					CAST(MIN(A.Date) AS date) AS StartDate,
					COUNT (DISTINCT CAST(A.Date AS date)) DaysPresent
				FROM [dbo].[Attendance] A
				WHERE A.IsDeleted = 0
				GROUP BY A.UserId
				) AS SQ2 ON SQ2.CandidateId = SQ1.CandidateId
			) SQ 
	ORDER BY SQ.CandidateId
'''

Fact_WorkAttendance_query = '''
	SELECT
		SQ.CandidateId,
		SQ.GroupId,
		SQ.Date,
		MIN(SQ.TimeIn) AS TimeIn,
		MAX(SQ.TimeOut) AS TimeOut,
		SUM(SQ.TotalHoursWorked) AS TotalHoursWorked
	FROM (
		SELECT
			A.UserId AS CandidateId,
			A.GroupId AS GroupId,
			CAST(A.Date AS date) AS Date,
			CAST(A.TimeIn AS time) AS TimeIn,
			CAST(A.TimeOut AS time) AS TimeOut,
			AP.NumberOfHourPerWeek,
			AP.NumberOfDayPerWeek,
			TotalHoursWorked =
				CASE
					WHEN A.TimeOut IS NULL THEN ISNULL(ROUND(AP.NumberOfHourPerWeek / NULLIF(AP.NumberOfDayPerWeek, 0), 1), 0)
					ELSE ROUND(CAST(DATEDIFF(minute, A.TimeIn, A.TimeOut) AS float) / 60.0, 1)
				END
		FROM [dbo].[Attendance] A
		INNER JOIN [dbo].[AttendancePattern] AP ON  AP.UserId = A.UserId AND AP.GroupId = A.GroupId
		WHERE 
			AP.IsDeleted = 0 AND
			A.IsDeleted = 0
		) AS SQ
	INNER JOIN [dbo].[User] U ON U.Id = SQ.CandidateId
	INNER JOIN [dbo].[Group] G ON G.Id = SQ.GroupId
	INNER JOIN [dbo].[ProjectEnrolmentRecord] PER ON PER.UserId = SQ.CandidateId AND PER.GroupId = SQ.GroupId
	WHERE
		U.IsDeleted = 0 AND U.IsDisabled = 0 AND U.FirstName IS NOT NULL AND
		G.IsDeleted = 0 AND
		PER.IsDeleted = 0 
	GROUP BY 
		SQ.CandidateId,
		SQ.GroupId,
		SQ.Date
	ORDER BY CandidateId ASC
'''

Fact_CandidateConversation_query = '''
	SELECT
		M.StarterId AS CandidateId,
		CAST(M.CreatedOn AS date) AS ConversationDate,
		ConversationRole = 'Starter',
		M.Rating
	FROM [dbo].[User] U
	LEFT JOIN [dbo].[ProjectEnrolmentRecord] PER ON PER.UserId = U.Id
	LEFT JOIN [dbo].[MatchingUserOptIn] M ON M.StarterId = U.Id
	WHERE 
		U.IsDeleted = 0 AND U.IsDisabled = 0 AND U.FirstName IS NOT NULL AND
		PER.IsDeleted = 0 AND
		M.IsCompleted = 1 AND M.IsDeleted = 0 AND M.StarterId IS NOT NULL
	UNION 
	SELECT
		M.PartnerId AS CandidateId,
		CAST(M.CreatedOn AS date) AS ConversationDate,
		ConversationRole = 'Partner',
		M.Rating
	FROM [dbo].[User] U
	LEFT JOIN [dbo].[ProjectEnrolmentRecord] PER ON PER.UserId = U.Id
	LEFT JOIN [dbo].[MatchingUserOptIn] M ON M.PartnerId = U.Id
	WHERE 
		U.IsDeleted = 0 AND U.IsDisabled = 0 AND U.FirstName IS NOT NULL AND
		PER.IsDeleted = 0 AND
		M.IsCompleted = 1 AND M.IsDeleted = 0 AND M.PartnerId IS NOT NULL
'''