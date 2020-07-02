SELECT MENU_NM AS "메뉴명"
		,BEFORE_MENU_NM AS "이전 메뉴명"
        ,COUNT(*) as "접근 건수" 
        ,TRUNCATE(COUNT(*) *100 /SUM(COUNT(*)) OVER (PARTITION BY MENU_NM) , 2) as "비율(%)"
FROM
(
	SELECT ordering.USR_NO,
			ordering.MENU_NM,
			ordering.ordering,
			before_ordering.MENU_NM as BEFORE_MENU_NM,
			before_ordering.ordering as before_ordering
	FROM
	(
		SELECT  USR_NO,
				MENU_NM ,
				ROW_NUMBER () OVER (PARTITION BY USR_NO ORDER BY LOG_TKTM) AS ORDERING
		FROM kakaobank.menu_log
	) ordering
	LEFT OUTER JOIN
	(
		SELECT  USR_NO,
				MENU_NM ,
				ROW_NUMBER () OVER (PARTITION BY USR_NO ORDER BY LOG_TKTM) AS ORDERING
		FROM kakaobank.menu_log
	) before_ordering
	ON ordering.ORDERING= before_ordering.ORDERING+1 AND ordering.USR_NO=before_ordering.USR_NO
	WHERE before_ordering.ORDERING IS NOT NULL
) static
GROUP BY MENU_NM, BEFORE_MENU_NM
ORDER BY MENU_NM, COUNT(*) DESC, BEFORE_MENU_NM
