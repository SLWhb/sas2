
PROC SQL;

CREATE TABLE trakhach._REZ_201210_201406_new (COMPRESS = BINARY) AS 
 SELECT rezalt.*, 
	/* перерасчитанный r_mark. Изменено только значение w_model_quality_veb_bki как w_model_quality_veb_bki_new */
	(CASE 
		WHEN rezalt.SC_ID_OLD=6 
		THEN rezalt.w_education 
		+ rezalt.w_household 
		+ rezalt.w_org_vid 
		+ rezalt.w_model_quality_veb_bki 
		+ rezalt.w_reg_groupe_new 
		+ rezalt.w_SD 
		+ rezalt.w_sex1_age 
		+ rezalt.w_specif 
		+ rezalt.w_stag 
		+ rezalt.w_autofilters 
		+ rezalt.w_channel_groups
		WHEN rezalt.SC_ID_OLD=7 
		THEN rezalt.w_d 
		+ rezalt.w_education 
		+ rezalt.w_household 
		+ rezalt.w_org_vid 
		+ rezalt.w_model_quality_veb_bki 
		+ rezalt.w_sex1_age 
		+ rezalt.w_stag 
		+ rezalt.w_autofilters 
		+ rezalt.w_bnk_type 
		+ rezalt.w_channel_groups 
		+ rezalt.w_specif
		WHEN rezalt.SC_ID_OLD=8
		THEN rezalt.w_d 
		+ rezalt.w_education 
		+ rezalt.w_household 
		+ rezalt.w_org_vid 
		+ rezalt.w_model_quality_veb_bki 
		+ rezalt.w_sex1_age 
		+ rezalt.w_specif 
		+ rezalt.w_stag 
		+ rezalt.w_autofilters 
		+ rezalt.w_bnk_type 
		+ rezalt.w_channel_groups
		WHEN rezalt.SC_ID_OLD=9
		THEN rezalt.w_age 
		+ rezalt.w_education 
		+ rezalt.w_count_kredit_debt 
		+ rezalt.w_od_sum_veb 
		+ rezalt.w_org_vid 
		+ rezalt.w_model_quality_veb_bki 
		+ rezalt.w_reg_groupe_new 
		+ rezalt.w_SD 
		+ rezalt.w_specif 
		+ rezalt.w_stag 
		+ rezalt.w_antiquity_veb 
		+ rezalt.w_autofilters 
		+ rezalt.w_channel_groups
		WHEN rezalt.SC_ID_OLD=10
		THEN rezalt.w_education 
		+ rezalt.w_household 
		+ rezalt.w_count_kredit_debt 
		+ rezalt.w_od_sum_veb 
		+ rezalt.w_org_vid 
		+ rezalt.w_model_quality_veb_bki 
		+ rezalt.w_reg_groupe_new 
		+ rezalt.w_SD 
		+ rezalt.w_sex1_age 
		+ rezalt.w_specif 
		+ rezalt.w_stag 
		+ rezalt.w_autofilters 
		+ rezalt.w_channel_groups
		WHEN rezalt.SC_ID_OLD=11
		THEN rezalt.w_cities_type 
		+ rezalt.w_d 
		+ rezalt.w_education 
		+ rezalt.w_count_kredit_debt 
		+ rezalt.w_od_sum_veb 
		+ rezalt.w_org_vid 
		+ rezalt.w_model_quality_veb_bki 
		+ rezalt.w_reg_groupe_new 
		+ rezalt.w_sex1_age 
		+ rezalt.w_specif 
		+ rezalt.w_stag 
		+ rezalt.w_autofilters 
		+ rezalt.w_l_ki_veb
		END) AS r_mark
	FROM (SELECT t1.*
				,(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=6 AND education = . THEN 23
					WHEN t1.SC_ID_OLD=6 AND education IN(1, 2) THEN 23
					WHEN t1.SC_ID_OLD=6 AND education IN(3, 5) THEN 9
					WHEN t1.SC_ID_OLD=6 AND education IN(4, 6) THEN 2
					WHEN t1.SC_ID_OLD=6 THEN 23
					WHEN t1.SC_ID_OLD=7 AND education = . THEN 13
					WHEN t1.SC_ID_OLD=7 AND education = 2 THEN 16
					WHEN t1.SC_ID_OLD=7 AND education = 5 THEN 13
					WHEN t1.SC_ID_OLD=7 AND education IN(1, 3, 4, 6) THEN 8
					WHEN t1.SC_ID_OLD=7 THEN 13
					WHEN t1.SC_ID_OLD=8 AND education = . THEN 7
					WHEN t1.SC_ID_OLD=8 AND education IN(4, 6) THEN 7
					WHEN t1.SC_ID_OLD=8 AND education IN(3, 5) THEN 13
					WHEN t1.SC_ID_OLD=8 AND education IN(1, 2) THEN 24
					WHEN t1.SC_ID_OLD=8 THEN 7
					WHEN t1.SC_ID_OLD=9 AND education = . THEN 11
					WHEN t1.SC_ID_OLD=9 AND education IN(3, 4, 6) THEN 5
					WHEN t1.SC_ID_OLD=9 AND education = 5 THEN 11
					WHEN t1.SC_ID_OLD=9 AND education IN(1, 2) THEN 18
					WHEN t1.SC_ID_OLD=9 THEN 11
					WHEN t1.SC_ID_OLD=10 AND education = . THEN 10
					WHEN t1.SC_ID_OLD=10 AND education IN(4, 6) THEN 5
					WHEN t1.SC_ID_OLD=10 AND education IN(3, 5) THEN 10
					WHEN t1.SC_ID_OLD=10 AND education IN(1, 2) THEN 23
					WHEN t1.SC_ID_OLD=10 THEN 10
					WHEN t1.SC_ID_OLD=11 AND education = . THEN 14
					WHEN t1.SC_ID_OLD=11 AND education IN(3, 4) THEN 8
					WHEN t1.SC_ID_OLD=11 AND education IN(5, 6) THEN 14
					WHEN t1.SC_ID_OLD=11 AND education IN(1, 2) THEN 19
					WHEN t1.SC_ID_OLD=11 THEN 14
				END) AS w_education,
				(CASE 
					WHEN t1.SC_id_new=6 AND education = . THEN 23
					WHEN t1.SC_id_new=6 AND education IN(1, 2) THEN 23
					WHEN t1.SC_id_new=6 AND education IN(3, 5) THEN 9
					WHEN t1.SC_id_new=6 AND education IN(4, 6) THEN 2
					WHEN t1.SC_id_new=6 THEN 23
					WHEN t1.SC_id_new=7 AND education = . THEN 13
					WHEN t1.SC_id_new=7 AND education = 2 THEN 16
					WHEN t1.SC_id_new=7 AND education = 5 THEN 13
					WHEN t1.SC_id_new=7 AND education IN(1, 3, 4, 6) THEN 8
					WHEN t1.SC_id_new=7 THEN 13
					WHEN t1.SC_id_new=8 AND education = . THEN 7
					WHEN t1.SC_id_new=8 AND education IN(4, 6) THEN 7
					WHEN t1.SC_id_new=8 AND education IN(3,5) THEN 13
					WHEN t1.SC_id_new=8 AND education IN(1,2) THEN 24
					WHEN t1.SC_id_new=8 THEN 7
					WHEN t1.SC_id_new=12 AND education = . THEN 11
					WHEN t1.SC_id_new=12 AND education IN(3,4,6) THEN 5
					WHEN t1.SC_id_new=12 AND education = 5 THEN 11
					WHEN t1.SC_id_new=12 AND education IN(1,2) THEN 18
					WHEN t1.SC_id_new=12 THEN 11
					WHEN t1.SC_id_new in(13) AND education = . THEN 4
					WHEN t1.SC_id_new in(13) AND education IN(4,6) THEN 4
					WHEN t1.SC_id_new in(13) AND education IN(1,3) THEN 8
					WHEN t1.SC_id_new in(13) AND education IN(5) THEN 9
					WHEN t1.SC_id_new in(13) AND education IN(2) THEN 18
					WHEN t1.SC_id_new in(13) THEN 4
					WHEN t1.SC_id_new=14 AND education = . THEN 10
					WHEN t1.SC_id_new=14 AND education IN(4,6) THEN 5
					WHEN t1.SC_id_new=14 AND education IN(3,5) THEN 10
					WHEN t1.SC_id_new=14 AND education IN(1,2) THEN 23
					WHEN t1.SC_id_new=14 THEN 10
					WHEN t1.SC_id_new in(15) AND education = . THEN 7
					WHEN t1.SC_id_new in(15) AND education IN(4,6) THEN 7
					WHEN t1.SC_id_new in(15) AND education IN(3,5) THEN 10
					WHEN t1.SC_id_new in(15) AND education IN(1,2) THEN 18
					WHEN t1.SC_id_new in(15) THEN 7
					WHEN t1.SC_id_new=16 AND education = . THEN 14
					WHEN t1.SC_id_new=16 AND education IN(3,4) THEN 8
					WHEN t1.SC_id_new=16 AND education IN(5,6) THEN 14
					WHEN t1.SC_id_new=16 AND education IN(1,2) THEN 19
					WHEN t1.SC_id_new=16 THEN 14
					WHEN t1.SC_id_new in(17) AND education = . THEN 7
					WHEN t1.SC_id_new in(17) AND education IN(3,4) THEN 7
					WHEN t1.SC_id_new in(17) AND education IN(1,5,6) THEN 14
					WHEN t1.SC_id_new in(17) AND education IN(2) THEN 15
					WHEN t1.SC_id_new in(17) THEN 7
				END) as w_education_new,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=6 AND household_id = . THEN 8
					WHEN t1.SC_ID_OLD=6 AND household_id IN(6, 7, 8) THEN 15
					WHEN t1.SC_ID_OLD=6 AND household_id IN(11, 12, 4, 5, 9) THEN 11
					WHEN t1.SC_ID_OLD=6 AND household_id IN(10, 2) THEN 8
					WHEN t1.SC_ID_OLD=6 AND household_id IN(1, 3) THEN 1
					WHEN t1.SC_ID_OLD=6 THEN 8
					WHEN t1.SC_ID_OLD=7 AND household_id = . THEN 12
					WHEN t1.SC_ID_OLD=7 AND household_id IN(1, 10, 2) THEN 6
					WHEN t1.SC_ID_OLD=7 AND household_id IN(11, 12, 3, 7) THEN 12
					WHEN t1.SC_ID_OLD=7 AND household_id IN(4, 5, 6, 8, 9) THEN 17
					WHEN t1.SC_ID_OLD=7 THEN 12
					WHEN t1.SC_ID_OLD=8 AND household_id = . THEN 2
					WHEN t1.SC_ID_OLD=8 AND household_id IN(1, 2) THEN 2
					WHEN t1.SC_ID_OLD=8 AND household_id IN(10, 11, 3, 5) THEN 10
					WHEN t1.SC_ID_OLD=8 AND household_id IN(12, 7, 8) THEN 16
					WHEN t1.SC_ID_OLD=8 AND household_id IN(4, 6, 9) THEN 21
					WHEN t1.SC_ID_OLD=8 THEN 2
					WHEN t1.SC_ID_OLD=10 AND household_id = . THEN 11
					WHEN t1.SC_ID_OLD=10 AND household_id IN(2, 3, 5, 6) THEN 10
					WHEN t1.SC_ID_OLD=10 AND household_id IN(1, 11, 9) THEN 11
					WHEN t1.SC_ID_OLD=10 AND household_id IN(10, 4, 8) THEN 12
					WHEN t1.SC_ID_OLD=10 AND household_id IN(12, 7) THEN 13
					WHEN t1.SC_ID_OLD=10 THEN 11
				END) AS w_household,
				(CASE 
					/* correct else */
					WHEN t1.SC_id_new=6 AND household_id = . THEN 8
					WHEN t1.SC_id_new=6 AND household_id IN(6, 7, 8) THEN 15
					WHEN t1.SC_id_new=6 AND household_id IN(11, 12, 4, 5, 9) THEN 11
					WHEN t1.SC_id_new=6 AND household_id IN(10, 2) THEN 8
					WHEN t1.SC_id_new=6 AND household_id IN(1, 3) THEN 1
					WHEN t1.SC_id_new=6 THEN 8
					WHEN t1.SC_id_new=7 AND household_id = . THEN 12
					WHEN t1.SC_id_new=7 AND household_id IN(1, 10, 2) THEN 6
					WHEN t1.SC_id_new=7 AND household_id IN(11, 12, 3, 7) THEN 12
					WHEN t1.SC_id_new=7 AND household_id IN(4, 5, 6, 8, 9) THEN 17
					WHEN t1.SC_id_new=7 THEN 12
					WHEN t1.SC_id_new=8 AND household_id = . THEN 2
					WHEN t1.SC_id_new=8 AND household_id IN(1, 2) THEN 2
					WHEN t1.SC_id_new=8 AND household_id IN(10, 11, 3, 5) THEN 10
					WHEN t1.SC_id_new=8 AND household_id IN(12, 7, 8) THEN 16
					WHEN t1.SC_id_new=8 AND household_id IN(4, 6, 9) THEN 21
					WHEN t1.SC_id_new=8 THEN 2
					WHEN t1.SC_id_new in(12,13) AND household_id = . THEN 6
					WHEN t1.SC_id_new in(12,13) AND household_id IN(2) THEN 6
					WHEN t1.SC_id_new in(12,13) AND household_id IN(1,10,11,12,3,5,7,8,9) THEN 9
					WHEN t1.SC_id_new in(12,13) AND household_id IN(4,6) THEN 11
					WHEN t1.SC_id_new in(12,13) THEN 6
					WHEN t1.SC_id_new=14 AND household_id = . THEN 11
					WHEN t1.SC_id_new=14 AND household_id IN(2, 3, 5, 6) THEN 10
					WHEN t1.SC_id_new=14 AND household_id IN(1, 11, 9) THEN 11
					WHEN t1.SC_id_new=14 AND household_id IN(10, 4, 8) THEN 12
					WHEN t1.SC_id_new=14 AND household_id IN(12, 7) THEN 13
					WHEN t1.SC_id_new=14 THEN 11
					WHEN t1.SC_id_new in(15) AND household_id = . THEN 7
					WHEN t1.SC_id_new in(15) AND household_id IN(2, 3, 5, 6) THEN 7
					WHEN t1.SC_id_new in(15) AND household_id IN(1, 11, 8, 9) THEN 9
					WHEN t1.SC_id_new in(15) AND household_id IN(10, 12, 4, 7) THEN 12
					WHEN t1.SC_id_new in(15) THEN 7
					WHEN t1.SC_id_new in(16,17) AND household_id = . THEN 9
					WHEN t1.SC_id_new in(16,17) AND household_id IN(10, 11, 12, 2, 3, 7) THEN 9
					WHEN t1.SC_id_new in(16,17) AND household_id IN(4, 8) THEN 13
					WHEN t1.SC_id_new in(16,17) AND household_id IN(1, 5, 6, 9) THEN 15
					WHEN t1.SC_id_new in(16,17) THEN 9
				END) AS w_household_new,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=6 AND org_vid = . THEN 10
					WHEN t1.SC_ID_OLD=6 AND org_vid IN(102, 103, 25, 28, 29, 31, 32, 33, 34, 35, 36, 37, 41, 43, 44, 48, 51, 58, 63, 65, 67, 68, 81, 99) THEN 5
					WHEN t1.SC_ID_OLD=6 AND org_vid IN(101, 22, 24, 26, 30, 39, 42, 45, 50, 53, 54, 55, 61, 62, 69, 70, 75, 79, 82, 90, 95, 98) THEN 10
					WHEN t1.SC_ID_OLD=6 AND org_vid IN(100, 104, 105, 17, 18, 19, 20, 21, 23, 27, 38, 40, 46, 47, 49, 56, 57, 59, 60, 64, 66, 71, 72, 73, 74, 76, 77, 78, 80, 83, 84, 85, 87, 88, 89, 91, 92, 93, 94, 96, 97) THEN 16
					WHEN t1.SC_ID_OLD=6 THEN 10
					WHEN t1.SC_ID_OLD=7 AND org_vid = . THEN 8
					WHEN t1.SC_ID_OLD=7 AND org_vid IN(20, 24, 29, 32, 34, 36, 39, 40, 48, 49, 51, 53, 58, 59, 80, 82, 85, 88, 98) THEN 2
					WHEN t1.SC_ID_OLD=7 AND org_vid IN(25, 26, 28, 30, 31, 33, 35, 37, 41, 43, 44, 47, 50, 52, 55, 56, 67, 68, 69, 73, 74, 77, 79, 83, 86, 87, 89, 96) THEN 8
					WHEN t1.SC_ID_OLD=7 AND org_vid IN(101, 102, 21, 22, 38, 45, 46, 54, 62, 66, 70, 71, 84, 93, 95, 99) THEN 11
					WHEN t1.SC_ID_OLD=7 AND org_vid IN(100, 103, 105, 17, 18, 19, 23, 27, 42, 57, 61, 63, 64, 65, 72, 75, 76, 78, 81, 90, 91, 92, 94, 97) THEN 17
					WHEN t1.SC_ID_OLD=7 THEN 8
					WHEN t1.SC_ID_OLD=8 AND org_vid = . THEN 16
					WHEN t1.SC_ID_OLD=8 AND org_vid IN(100, 102, 17, 18, 20, 35, 39, 67, 72, 73, 74, 76, 78, 79, 85, 87, 90, 91, 92, 93, 94) THEN 21
					WHEN t1.SC_ID_OLD=8 AND org_vid IN(101, 103, 21, 23, 26, 37, 40, 42, 44, 45, 50, 52, 56, 60, 61, 71, 75, 82, 83, 84, 95, 96, 97) THEN 16
					WHEN t1.SC_ID_OLD=8 AND org_vid IN(105, 19, 22, 24, 25, 38, 46, 49, 53, 54, 57, 58, 62, 66, 68, 70, 77, 99) THEN 10
					WHEN t1.SC_ID_OLD=8 AND org_vid IN(28, 29, 30, 31, 32, 33, 34, 36, 41, 43, 48, 51, 55, 59, 63, 64, 65, 69, 80, 81, 88, 98) THEN 3
					WHEN t1.SC_ID_OLD=8 THEN 16
					WHEN t1.SC_ID_OLD=9 AND org_vid = . THEN 11
					WHEN t1.SC_ID_OLD=9 AND org_vid IN(100, 101, 19, 26, 39, 63, 72, 74, 76, 78, 88, 90, 92, 93, 94, 98) THEN 15
					WHEN t1.SC_ID_OLD=9 AND org_vid IN(102, 17, 20, 21, 23, 29, 33, 36, 37, 38, 40, 42, 44, 49, 52, 53, 56, 60, 61, 62, 64, 65, 71, 77, 82, 84, 85, 86, 87, 91, 95, 97) THEN 11
					WHEN t1.SC_ID_OLD=9 AND org_vid IN(103, 105, 18, 22, 24, 25, 27, 28, 30, 31, 32, 34, 35, 41, 43, 45, 46, 48, 50, 51, 54, 55, 57, 58, 59, 66, 67, 68, 69, 70, 73, 75, 79, 80, 81, 96, 99) THEN 8
					WHEN t1.SC_ID_OLD=9 THEN 11
					WHEN t1.SC_ID_OLD=10 AND org_vid = . THEN 9
					WHEN t1.SC_ID_OLD=10 AND org_vid IN(23, 25, 27, 28, 29, 31, 33, 34, 36, 37, 41, 42, 43, 44, 45, 48, 49, 50, 51, 52, 53, 55, 56, 57, 58, 60, 61, 63, 65, 67, 68, 69, 70, 74, 76, 81, 83, 84, 86, 87, 95, 97, 98, 99) THEN 9
					WHEN t1.SC_ID_OLD=10 AND org_vid IN(101, 102, 105, 18, 19, 21, 22, 24, 30, 38, 40, 54, 62, 66, 71, 75, 79) THEN 12
					WHEN t1.SC_ID_OLD=10 AND org_vid IN(0, 100, 103, 17, 20, 26, 32, 35, 39, 46, 59, 64, 72, 73, 77, 78, 80, 82, 85, 88, 90, 91, 92, 93, 94, 96) THEN 20
					WHEN t1.SC_ID_OLD=10 THEN 9
					WHEN t1.SC_ID_OLD=11 AND org_vid = . THEN 7
					WHEN t1.SC_ID_OLD=11 AND org_vid IN(0, 101, 102, 103, 20, 22, 25, 28, 29, 32, 34, 35, 36, 37, 38, 41, 43, 48, 49, 51, 53, 54, 58, 60, 63, 65, 66, 67, 68, 70, 78, 80, 81, 82, 83, 84, 85, 87, 88, 93, 95, 96, 99) THEN 7
					WHEN t1.SC_ID_OLD=11 AND org_vid IN(100, 21, 23, 26, 30, 31, 42, 45, 55, 61, 62, 69, 71, 73, 75, 90, 91, 94) THEN 15
					WHEN t1.SC_ID_OLD=11 AND org_vid IN(105, 17, 18, 19, 24, 33, 39, 40, 46, 50, 57, 59, 64, 72, 74, 76, 79, 92, 98) THEN 27
					WHEN t1.SC_ID_OLD=11 THEN 7
				END) AS w_org_vid,
				(CASE 
					WHEN t1.SC_id_new=6 AND org_vid = . THEN 10
					WHEN t1.SC_id_new=6 AND org_vid IN(102, 103, 25, 28, 29, 31, 32, 33, 34, 35, 36, 37, 41, 43, 44, 48, 51, 58, 63, 65, 67, 68, 81, 99) THEN 5
					WHEN t1.SC_id_new=6 AND org_vid IN(101, 22, 24, 26, 30, 39, 42, 45, 50, 53, 54, 55, 61, 62, 69, 70, 75, 79, 82, 90, 95, 98) THEN 10
					WHEN t1.SC_id_new=6 AND org_vid IN(100, 104, 105, 17, 18, 19, 20, 21, 23, 27, 38, 40, 46, 47, 49, 56, 57, 59, 60, 64, 66, 71, 72, 73, 74, 76, 77, 78, 80, 83, 84, 85, 87, 88, 89, 91, 92, 93, 94, 96, 97) THEN 16
					WHEN t1.SC_id_new=6 THEN 10
					WHEN t1.SC_id_new=7 AND org_vid = . THEN 8
					WHEN t1.SC_id_new=7 AND org_vid IN(20, 24, 29, 32, 34, 36, 39, 40, 48, 49, 51, 53, 58, 59, 80, 82, 85, 88, 98) THEN 2
					WHEN t1.SC_id_new=7 AND org_vid IN(25, 26, 28, 30, 31, 33, 35, 37, 41, 43, 44, 47, 50, 52, 55, 56, 67, 68, 69, 73, 74, 77, 79, 83, 86, 87, 89, 96) THEN 8
					WHEN t1.SC_id_new=7 AND org_vid IN(101, 102, 21, 22, 38, 45, 46, 54, 62, 66, 70, 71, 84, 93, 95, 99) THEN 11
					WHEN t1.SC_id_new=7 AND org_vid IN(100, 103, 105, 17, 18, 19, 23, 27, 42, 57, 61, 63, 64, 65, 72, 75, 76, 78, 81, 90, 91, 92, 94, 97) THEN 17
					WHEN t1.SC_id_new=7 THEN 8
					WHEN t1.SC_id_new=8 AND org_vid = . THEN 16
					WHEN t1.SC_id_new=8 AND org_vid IN(100, 102, 17, 18, 20, 35, 39, 67, 72, 73, 74, 76, 78, 79, 85, 87, 90, 91, 92, 93, 94) THEN 21
					WHEN t1.SC_id_new=8 AND org_vid IN(101, 103, 21, 23, 26, 37, 40, 42, 44, 45, 50, 52, 56, 60, 61, 71, 75, 82, 83, 84, 95, 96, 97) THEN 16
					WHEN t1.SC_id_new=8 AND org_vid IN(105, 19, 22, 24, 25, 38, 46, 49, 53, 54, 57, 58, 62, 66, 68, 70, 77, 99) THEN 10
					WHEN t1.SC_id_new=8 AND org_vid IN(28, 29, 30, 31, 32, 33, 34, 36, 41, 43, 48, 51, 55, 59, 63, 64, 65, 69, 80, 81, 88, 98) THEN 3
					WHEN t1.SC_id_new=8 THEN 16
					WHEN t1.SC_id_new=12 AND org_vid = . THEN 11
					WHEN t1.SC_id_new=12 AND org_vid IN(100, 101, 19, 26, 39, 63, 72, 74, 76, 78, 88, 90, 92, 93, 94, 98) THEN 15
					WHEN t1.SC_id_new=12 AND org_vid IN(102, 17, 20, 21, 23, 29, 33, 36, 37, 38, 40, 42, 44, 49, 52, 53, 56, 60, 61, 62, 64, 65, 71, 77, 82, 84, 85, 86, 87, 91, 95, 97) THEN 11
					WHEN t1.SC_id_new=12 AND org_vid IN(103, 105, 18, 22, 24, 25, 27, 28, 30, 31, 32, 34, 35, 41, 43, 45, 46, 48, 50, 51, 54, 55, 57, 58, 59, 66, 67, 68, 69, 70, 73, 75, 79, 80, 81, 96, 99) THEN 8
					WHEN t1.SC_id_new=12 THEN 11
					WHEN t1.SC_id_new in(13) AND org_vid = . THEN 16
					WHEN t1.SC_id_new in(13) AND org_vid IN(0, 100, 101, 102, 17, 18, 19, 20, 26, 37, 39, 42, 49, 52, 56, 60, 63, 64, 65, 72, 76, 78, 82, 83, 84, 85, 87, 88, 90, 92, 93, 94, 96, 97, 98) THEN 16
					WHEN t1.SC_id_new in(13) AND org_vid IN(21, 22, 23, 27, 43, 46, 53, 54, 59, 62, 70, 71, 91, 95) THEN 8
					WHEN t1.SC_id_new in(13) AND org_vid IN(103, 105, 24, 25, 28, 29, 30, 31, 32, 33, 34, 35, 36, 38, 40, 41, 45, 48, 50, 51, 55, 57, 58, 61, 66, 67, 68, 69, 73, 74, 75, 79, 80, 81, 99) THEN 5
					WHEN t1.SC_id_new in(13) THEN 16
					WHEN t1.SC_id_new=14 AND org_vid = . THEN 9
					WHEN t1.SC_id_new=14 AND org_vid IN(23, 25, 27, 28, 29, 31, 33, 34, 36, 37, 41, 42, 43, 44, 45, 48, 49, 50, 51, 52, 53, 55, 56, 57, 58, 60, 61, 63, 65, 67, 68, 69, 70, 74, 76, 81, 83, 84, 86, 87, 95, 97, 98, 99) THEN 9
					WHEN t1.SC_id_new=14 AND org_vid IN(101, 102, 105, 18, 19, 21, 22, 24, 30, 38, 40, 54, 62, 66, 71, 75, 79) THEN 12
					WHEN t1.SC_id_new=14 AND org_vid IN(0, 100, 103, 17, 20, 26, 32, 35, 39, 46, 59, 64, 72, 73, 77, 78, 80, 82, 85, 88, 90, 91, 92, 93, 94, 96) THEN 20
					WHEN t1.SC_id_new=14 THEN 9
					WHEN t1.SC_id_new in(15) AND org_vid = . THEN 18
					WHEN t1.SC_id_new in(15) AND org_vid IN(24, 25, 27, 28, 29, 30, 31, 33, 34, 36, 38, 40, 41, 42, 43, 48, 51, 58, 65, 68, 69, 74, 80, 81, 84, 85, 95, 98) THEN 5
					WHEN t1.SC_id_new in(15) AND org_vid IN(101, 102, 105, 17, 22, 23, 26, 45, 50, 53, 54, 55, 57, 59, 61, 62, 66, 67, 70, 75, 76, 79, 93, 99) THEN 11
					WHEN t1.SC_id_new in(15) AND org_vid IN(0, 100, 103, 18, 19, 20, 21, 32, 35, 37, 39, 46, 47, 49, 52, 56, 60, 63, 64, 71, 72, 73, 77, 78, 82, 83, 86, 87, 88, 90, 91, 92, 94, 96, 97) THEN 18
					WHEN t1.SC_id_new in(15) THEN 18
					WHEN t1.SC_id_new=16 AND org_vid = . THEN 7
					WHEN t1.SC_id_new=16 AND org_vid IN(0, 101, 102, 103, 20, 22, 25, 28, 29, 32, 34, 35, 36, 37, 38, 41, 43, 48, 49, 51, 53, 54, 58, 60, 63, 65, 66, 67, 68, 70, 78, 80, 81, 82, 83, 84, 85, 87, 88, 93, 95, 96, 99) THEN 7
					WHEN t1.SC_id_new=16 AND org_vid IN(100, 21, 23, 26, 30, 31, 42, 45, 55, 61, 62, 69, 71, 73, 75, 90, 91, 94) THEN 15
					WHEN t1.SC_id_new=16 AND org_vid IN(105, 17, 18, 19, 24, 33, 39, 40, 46, 50, 57, 59, 64, 72, 74, 76, 79, 92, 98) THEN 27
					WHEN t1.SC_id_new=16 THEN 7
					WHEN t1.SC_id_new in(17) AND org_vid = . THEN 31
					WHEN t1.SC_id_new in(17) AND org_vid IN(22, 29, 34, 35, 40, 41, 43, 45, 46, 48, 49, 51, 53, 54, 58, 63, 66, 68, 69, 70, 81, 93, 95, 99) THEN 5
					WHEN t1.SC_id_new in(17) AND org_vid IN(100, 101, 102, 17, 21, 23, 24, 26, 28, 30, 31, 38, 39, 42, 50, 55, 57, 62, 71, 72, 73, 76, 78) THEN 14
					WHEN t1.SC_id_new in(17) AND org_vid IN(0, 103, 105, 18, 19, 20, 25, 32, 33, 36, 59, 60, 61, 64, 65, 67, 74, 75, 79, 80, 83, 84, 85, 87, 88, 90, 91, 92, 94, 96, 98) THEN 31
					WHEN t1.SC_id_new in(17) THEN 31
				END) as w_org_vid_new,
				/* (CASE 
					WHEN t1.SC_ID_OLD=6AND model_quality_veb_bki< 132 THEN -3
					WHEN t1.SC_ID_OLD=6AND model_quality_veb_bki>= 132 AND model_quality_veb_bki< 147 THEN 6
					WHEN t1.SC_ID_OLD=6AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 158 THEN 17
					WHEN t1.SC_ID_OLD=6AND model_quality_veb_bki>= 158 THEN 32
					WHEN t1.SC_ID_OLD=6 THEN 6
					WHEN t1.SC_ID_OLD=7 AND model_quality_veb_bki< 139 THEN -1
					WHEN t1.SC_ID_OLD=7 AND model_quality_veb_bki>= 139 AND model_quality_veb_bki<159 THEN 13
					WHEN t1.SC_ID_OLD=7 AND model_quality_veb_bki>= 159 THEN 31
					WHEN t1.SC_ID_OLD=7 THEN -1
					WHEN t1.SC_ID_OLD=8 AND model_quality_veb_bki< 144 THEN -1
					WHEN t1.SC_ID_OLD=8 AND model_quality_veb_bki>= 144 AND model_quality_veb_bki<158 THEN 11
					WHEN t1.SC_ID_OLD=8 AND model_quality_veb_bki>= 158 THEN 31
					WHEN t1.SC_ID_OLD=8 THEN 11
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki< 125 THEN -7
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki>= 125 AND model_quality_veb_bki< 144 THEN 4
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 156 THEN 13
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki>= 156 THEN 29
					WHEN t1.SC_ID_OLD=9 THEN 13
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki< 142 THEN -7
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki>= 142 AND model_quality_veb_bki< 147 THEN 10
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 169 THEN 13
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki>= 169 THEN 35
					WHEN t1.SC_ID_OLD=10 THEN 13
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki< 144 THEN -6
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki>= 144 AND model_quality_veb_bki< 147 THEN 7
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 169 THEN 17
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki>= 169 THEN 33
					WHEN t1.SC_ID_OLD=11 THEN 7
				END) AS w_model_quality_veb_bki_old, */
				/* перерасчитанный параметр веса w_model_quality_veb_bki*/
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=6 AND model_quality_veb_bki = . THEN 6
					WHEN t1.SC_ID_OLD=6 AND model_quality_veb_bki< 132 THEN -3
					WHEN t1.SC_ID_OLD=6 AND model_quality_veb_bki>= 132 AND model_quality_veb_bki< 147 THEN 6
					WHEN t1.SC_ID_OLD=6 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 158 THEN 17
					WHEN t1.SC_ID_OLD=6 AND model_quality_veb_bki>= 158 THEN 32
					WHEN t1.SC_ID_OLD=6 THEN 6
					WHEN t1.SC_ID_OLD=7 AND model_quality_veb_bki = . THEN -1
					WHEN t1.SC_ID_OLD=7 AND model_quality_veb_bki< 139 THEN -1
					WHEN t1.SC_ID_OLD=7 AND model_quality_veb_bki>= 139 AND model_quality_veb_bki<159 THEN 13
					WHEN t1.SC_ID_OLD=7 AND model_quality_veb_bki>= 159 THEN 31
					WHEN t1.SC_ID_OLD=7 THEN -1
					WHEN t1.SC_ID_OLD=8 AND model_quality_veb_bki = . THEN 11
					WHEN t1.SC_ID_OLD=8 AND model_quality_veb_bki< 144 THEN -1
					WHEN t1.SC_ID_OLD=8 AND model_quality_veb_bki>= 144 AND model_quality_veb_bki<158 THEN 11
					WHEN t1.SC_ID_OLD=8 AND model_quality_veb_bki>= 158 THEN 31
					WHEN t1.SC_ID_OLD=8 THEN 11
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki = . THEN 13
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki< 125 THEN -7
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki>= 125 AND model_quality_veb_bki< 147 /*было 144*/ THEN 4
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 156 THEN 13
					WHEN t1.SC_ID_OLD=9 AND model_quality_veb_bki>= 156 THEN 29
					WHEN t1.SC_ID_OLD=9 THEN 13
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki = . THEN 13
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki< 142 THEN -7
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki>= 142 AND model_quality_veb_bki< 147 THEN 10
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 169 THEN 13
					WHEN t1.SC_ID_OLD=10 AND model_quality_veb_bki>= 169 THEN 35
					WHEN t1.SC_ID_OLD=10 THEN 13
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki = . THEN 7
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki< 144 THEN -6
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki>= 144 AND model_quality_veb_bki< 147 THEN 7
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 169 THEN 17
					WHEN t1.SC_ID_OLD=11 AND model_quality_veb_bki>= 169 THEN 33
					WHEN t1.SC_ID_OLD=11 THEN 7
				END) AS w_model_quality_veb_bki,
				(CASE
					WHEN t1.SC_id_new=6 AND model_quality_veb_bki = . THEN 6
					WHEN t1.SC_id_new=6 AND model_quality_veb_bki< 132 THEN -3
					WHEN t1.SC_id_new=6 AND model_quality_veb_bki>= 132 AND model_quality_veb_bki< 147 THEN 6
					WHEN t1.SC_id_new=6 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 158 THEN 17
					WHEN t1.SC_id_new=6 AND model_quality_veb_bki>= 158 THEN 32
					WHEN t1.SC_id_new=6 THEN 6
					WHEN t1.SC_id_new=7 AND model_quality_veb_bki = . THEN -1
					WHEN t1.SC_id_new=7 AND model_quality_veb_bki< 139 THEN -1
					WHEN t1.SC_id_new=7 AND model_quality_veb_bki>= 139 AND model_quality_veb_bki<159 THEN 13
					WHEN t1.SC_id_new=7 AND model_quality_veb_bki>= 159 THEN 31
					WHEN t1.SC_id_new=7 THEN -1
					WHEN t1.SC_id_new=8 AND model_quality_veb_bki = . THEN 11
					WHEN t1.SC_id_new=8 AND model_quality_veb_bki< 144 THEN -1
					WHEN t1.SC_id_new=8 AND model_quality_veb_bki>= 144 AND model_quality_veb_bki<158 THEN 11
					WHEN t1.SC_id_new=8 AND model_quality_veb_bki>= 158 THEN 31
					WHEN t1.SC_id_new=8 THEN 11
					WHEN t1.SC_id_new=12 AND model_quality_veb_bki = . THEN 13
					WHEN t1.SC_id_new=12 AND model_quality_veb_bki< 125 THEN -7
					WHEN t1.SC_id_new=12 AND model_quality_veb_bki>= 125 AND model_quality_veb_bki< 147 /*было 144*/ THEN 4
					WHEN t1.SC_id_new=12 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 156 THEN 13
					WHEN t1.SC_id_new=12 AND model_quality_veb_bki>= 156 THEN 29
					WHEN t1.SC_id_new=12 THEN 13
					WHEN t1.SC_id_new in(13) AND model_quality_veb_bki = . THEN 4
					WHEN t1.SC_id_new in(13) AND model_quality_veb_bki< 125 THEN -8
					WHEN t1.SC_id_new in(13) AND model_quality_veb_bki>= 125 AND model_quality_veb_bki< 147 THEN 4
					WHEN t1.SC_id_new in(13) AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 155 THEN 15
					WHEN t1.SC_id_new in(13) AND model_quality_veb_bki>= 155 THEN 26
					WHEN t1.SC_id_new IN(13) THEN 4
					WHEN t1.SC_id_new=14 AND model_quality_veb_bki = . THEN 13
					WHEN t1.SC_id_new=14 AND model_quality_veb_bki< 142 THEN -7
					WHEN t1.SC_id_new=14 AND model_quality_veb_bki>= 142 AND model_quality_veb_bki< 147 THEN 10
					WHEN t1.SC_id_new=14 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 169 THEN 13
					WHEN t1.SC_id_new=14 AND model_quality_veb_bki>= 169 THEN 35
					WHEN t1.SC_id_new=14 THEN 13
					WHEN t1.SC_id_new in(15) AND model_quality_veb_bki = . THEN 10
					WHEN t1.SC_id_new in(15) AND model_quality_veb_bki< 141 THEN -5
					WHEN t1.SC_id_new in(15) AND model_quality_veb_bki>= 141 AND model_quality_veb_bki< 156 THEN 10
					WHEN t1.SC_id_new in(15) AND model_quality_veb_bki>= 156 AND model_quality_veb_bki< 170 THEN 21
					WHEN t1.SC_id_new in(15) AND model_quality_veb_bki>= 170 THEN 31
					WHEN t1.SC_id_new IN(15) THEN 10
					WHEN t1.SC_id_new=16 AND model_quality_veb_bki = . THEN 7
					WHEN t1.SC_id_new=16 AND model_quality_veb_bki< 144 THEN -6
					WHEN t1.SC_id_new=16 AND model_quality_veb_bki>= 144 AND model_quality_veb_bki< 147 THEN 7
					WHEN t1.SC_id_new=16 AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 169 THEN 17
					WHEN t1.SC_id_new=16 AND model_quality_veb_bki>= 169 THEN 33
					WHEN t1.SC_id_new=16 THEN 7
					WHEN t1.SC_id_new in(17) AND model_quality_veb_bki = . THEN 8
					WHEN t1.SC_id_new in(17) AND model_quality_veb_bki< 144 THEN -5
					WHEN t1.SC_id_new in(17) AND model_quality_veb_bki>= 144 AND model_quality_veb_bki< 147 THEN 8
					WHEN t1.SC_id_new in(17) AND model_quality_veb_bki>= 147 AND model_quality_veb_bki< 169 THEN 14
					WHEN t1.SC_id_new in(17) AND model_quality_veb_bki>= 169 THEN 31
					WHEN t1.SC_id_new IN(17) THEN 8
				END) as w_model_quality_veb_bki_n,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=6 AND reg_groupe_new = . THEN 15
					WHEN t1.SC_ID_OLD=6 AND reg_groupe_new IN(1, 2) THEN 15
					WHEN t1.SC_ID_OLD=6 AND reg_groupe_new = 3 THEN 1
					WHEN t1.SC_ID_OLD=6 THEN 15
					WHEN t1.SC_ID_OLD=9 AND reg_groupe_new = . THEN 11
					WHEN t1.SC_ID_OLD=9 AND reg_groupe_new = 3 THEN 2
					WHEN t1.SC_ID_OLD=9 AND reg_groupe_new = 2 THEN 11
					WHEN t1.SC_ID_OLD=9 AND reg_groupe_new = 1 THEN 15
					WHEN t1.SC_ID_OLD=9 THEN 11
					WHEN t1.SC_ID_OLD=10 AND reg_groupe_new = . THEN 15
					WHEN t1.SC_ID_OLD=10 AND reg_groupe_new = 3 THEN 7
					WHEN t1.SC_ID_OLD=10 AND reg_groupe_new = 2 THEN 7
					WHEN t1.SC_ID_OLD=10 AND reg_groupe_new = 1 THEN 15
					WHEN t1.SC_ID_OLD=10 THEN 15
					WHEN t1.SC_ID_OLD=11 AND reg_groupe_new = . THEN 9
					WHEN t1.SC_ID_OLD=11 AND reg_groupe_new = 3 THEN 9
					WHEN t1.SC_ID_OLD=11 AND reg_groupe_new = 2 THEN 9
					WHEN t1.SC_ID_OLD=11 AND reg_groupe_new = 1 THEN 16
					WHEN t1.SC_ID_OLD=11 THEN 9
				END) AS w_reg_groupe_new,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=6 AND sd = . THEN 10
					WHEN t1.SC_ID_OLD=6 AND sd < 0.04 THEN 14
					WHEN t1.SC_ID_OLD=6 AND sd >= 0.04 AND sd < 0.06 THEN 12
					WHEN t1.SC_ID_OLD=6 AND sd >= 0.06 AND sd < 0.09 THEN 10
					WHEN t1.SC_ID_OLD=6 AND sd >= 0.09 AND sd < 0.12 THEN 8
					WHEN t1.SC_ID_OLD=6 AND sd >= 0.12 THEN 5
					WHEN t1.SC_ID_OLD=6 THEN 10
					WHEN t1.SC_ID_OLD=9 AND sd = . THEN 13
					WHEN t1.SC_ID_OLD=9 AND sd < 0.02 THEN 13
					WHEN t1.SC_ID_OLD=9 AND sd >= 0.02 AND sd < 0.04 THEN 11
					WHEN t1.SC_ID_OLD=9 AND sd >= 0.04 AND sd < 0.07 THEN 9
					WHEN t1.SC_ID_OLD=9 AND sd >= 0.07 THEN 6
					WHEN t1.SC_ID_OLD=9 THEN 13
					WHEN t1.SC_ID_OLD=10 AND sd = . THEN 14
					WHEN t1.SC_ID_OLD=10 AND sd < 0.03 THEN 14
					WHEN t1.SC_ID_OLD=10 AND sd >= 0.03 AND sd < 0.04 THEN 13
					WHEN t1.SC_ID_OLD=10 AND sd >= 0.04 AND sd < 0.06 THEN 12
					WHEN t1.SC_ID_OLD=10 AND sd >= 0.06 THEN 10
					WHEN t1.SC_ID_OLD=10 THEN 14
				END) AS w_SD,
				(CASE
					WHEN t1.SC_id_new=6 AND sd = . THEN 10
					WHEN t1.SC_id_new=6 AND sd < 0.04 THEN 14
					WHEN t1.SC_id_new=6 AND sd >= 0.04 AND sd < 0.06 THEN 12
					WHEN t1.SC_id_new=6 AND sd >= 0.06 AND sd < 0.09 THEN 10
					WHEN t1.SC_id_new=6 AND sd >= 0.09 AND sd < 0.12 THEN 8
					WHEN t1.SC_id_new=6 AND sd >= 0.12 THEN 5
					WHEN t1.SC_id_new=6 THEN 10
					WHEN t1.SC_id_new=12 AND sd = . THEN 13
					WHEN t1.SC_id_new=12 AND sd < 0.02 THEN 13
					WHEN t1.SC_id_new=12 AND sd >= 0.02 AND sd < 0.04 THEN 11
					WHEN t1.SC_id_new=12 AND sd >= 0.04 AND sd < 0.07 THEN 9
					WHEN t1.SC_id_new=12 AND sd >= 0.07 THEN 6
					WHEN t1.SC_id_new=12 THEN 13
					WHEN t1.SC_id_new in(13) AND sd = . THEN 12
					WHEN t1.SC_id_new in(13) AND sd < 0.02 THEN 12
					WHEN t1.SC_id_new in(13) AND sd >= 0.02 AND sd < 0.04 THEN 10
					WHEN t1.SC_id_new in(13) AND sd >= 0.04 AND sd < 0.06 THEN 8
					WHEN t1.SC_id_new in(13) AND sd >= 0.06 AND sd < 0.09 THEN 6
					WHEN t1.SC_id_new in(13) AND sd >= 0.09 THEN 5
					WHEN t1.SC_id_new in(13) THEN 12
					WHEN t1.SC_id_new=14 AND sd = . THEN 14
					WHEN t1.SC_id_new=14 AND sd < 0.03 THEN 14
					WHEN t1.SC_id_new=14 AND sd >= 0.03 AND sd < 0.04 THEN 13
					WHEN t1.SC_id_new=14 AND sd >= 0.04 AND sd < 0.06 THEN 12
					WHEN t1.SC_id_new=14 AND sd >= 0.06 THEN 10
					WHEN t1.SC_id_new=14 THEN 14
					WHEN t1.SC_id_new in(15) AND sd = . THEN 13
					WHEN t1.SC_id_new in(15) AND sd < 0.03 THEN 13
					WHEN t1.SC_id_new in(15) AND sd >= 0.03 AND sd < 0.04 THEN 12
					WHEN t1.SC_id_new in(15) AND sd >= 0.04 AND sd < 0.06 THEN 11
					WHEN t1.SC_id_new in(15) AND sd >= 0.06 AND sd < 0.08 THEN 10
					WHEN t1.SC_id_new in(15) AND sd >= 0.08 THEN 7
					WHEN t1.SC_id_new in(15) THEN 13
				END) as w_sd_new,
				(CASE 
					/* correct else*/
					WHEN t1.SC_ID_OLD=6 AND sex1_age = . THEN 9
					WHEN t1.SC_ID_OLD=6 AND sex1_age < 32 THEN 9
					WHEN t1.SC_ID_OLD=6 AND sex1_age >= 32 AND sex1_age < 44 THEN 10
					WHEN t1.SC_ID_OLD=6 AND sex1_age >= 44 AND sex1_age < 54 THEN 9
					WHEN t1.SC_ID_OLD=6 AND sex1_age >= 54 AND sex1_age < 64 THEN 11
					WHEN t1.SC_ID_OLD=6 AND sex1_age >= 64 THEN 12
					WHEN t1.SC_ID_OLD=6 THEN 9
					WHEN t1.SC_ID_OLD=7 AND sex1_age = . THEN 8
					WHEN t1.SC_ID_OLD=7 AND sex1_age < 74 THEN 8
					WHEN t1.SC_ID_OLD=7 AND sex1_age >= 74 AND sex1_age < 104 THEN 10
					WHEN t1.SC_ID_OLD=7 AND sex1_age >= 104 AND sex1_age < 114 THEN 15
					WHEN t1.SC_ID_OLD=7 AND sex1_age >= 114 THEN 17
					WHEN t1.SC_ID_OLD=7 THEN 8
					WHEN t1.SC_ID_OLD=8 AND sex1_age = . THEN 11
					WHEN t1.SC_ID_OLD=8 AND sex1_age < 52 THEN 6
					WHEN t1.SC_ID_OLD=8 AND sex1_age >= 52 AND sex1_age < 84 THEN 11
					WHEN t1.SC_ID_OLD=8 AND sex1_age >= 84 AND sex1_age < 104 THEN 13
					WHEN t1.SC_ID_OLD=8 AND sex1_age >= 104 THEN 20
					WHEN t1.SC_ID_OLD=8 THEN 11
					WHEN t1.SC_ID_OLD=10 AND sex1_age = . THEN 10
					WHEN t1.SC_ID_OLD=10 AND sex1_age < 32 THEN 5
					WHEN t1.SC_ID_OLD=10 AND sex1_age >= 32 AND sex1_age < 64 THEN 10
					WHEN t1.SC_ID_OLD=10 AND sex1_age >= 64 AND sex1_age < 84 THEN 13
					WHEN t1.SC_ID_OLD=10 AND sex1_age >= 84 AND sex1_age < 94 THEN 16
					WHEN t1.SC_ID_OLD=10 AND sex1_age >= 94 THEN 19
					WHEN t1.SC_ID_OLD=10 THEN 10
					WHEN t1.SC_ID_OLD=11 AND sex1_age = . THEN 6
					WHEN t1.SC_ID_OLD=11 AND sex1_age < 57 THEN 2
					WHEN t1.SC_ID_OLD=11 AND sex1_age >= 57 AND sex1_age < 104 THEN 6
					WHEN t1.SC_ID_OLD=11 AND sex1_age >= 104 THEN 17
					WHEN t1.SC_ID_OLD=11 THEN 6
				END) AS w_sex1_age,
				(CASE
					WHEN t1.SC_id_new=6 AND sex1_age = . THEN 9
					WHEN t1.SC_id_new=6 AND sex1_age < 32 THEN 9
					WHEN t1.SC_id_new=6 AND sex1_age >= 32 AND sex1_age < 44 THEN 10
					WHEN t1.SC_id_new=6 AND sex1_age >= 44 AND sex1_age < 54 THEN 9
					WHEN t1.SC_id_new=6 AND sex1_age >= 54 AND sex1_age < 64 THEN 11
					WHEN t1.SC_id_new=6 AND sex1_age >= 64 THEN 12
					WHEN t1.SC_id_new=6 THEN 9
					WHEN t1.SC_id_new=7 AND sex1_age = . THEN 8
					WHEN t1.SC_id_new=7 AND sex1_age < 74 THEN 8
					WHEN t1.SC_id_new=7 AND sex1_age >= 74 AND sex1_age < 104 THEN 10
					WHEN t1.SC_id_new=7 AND sex1_age >= 104 AND sex1_age < 114 THEN 15
					WHEN t1.SC_id_new=7 AND sex1_age >= 114 THEN 17
					WHEN t1.SC_id_new=7 THEN 8
					WHEN t1.SC_id_new=8 AND sex1_age = . THEN 11
					WHEN t1.SC_id_new=8 AND sex1_age < 52 THEN 6
					WHEN t1.SC_id_new=8 AND sex1_age >= 52 AND sex1_age < 84 THEN 11
					WHEN t1.SC_id_new=8 AND sex1_age >= 84 AND sex1_age < 104 THEN 13
					WHEN t1.SC_id_new=8 AND sex1_age >= 104 THEN 20
					WHEN t1.SC_id_new=8 THEN 11
					WHEN t1.SC_id_new=14 AND sex1_age = . THEN 10
					WHEN t1.SC_id_new=14 AND sex1_age < 32 THEN 5
					WHEN t1.SC_id_new=14 AND sex1_age >= 32 AND sex1_age < 64 THEN 10
					WHEN t1.SC_id_new=14 AND sex1_age >= 64 AND sex1_age < 84 THEN 13
					WHEN t1.SC_id_new=14 AND sex1_age >= 84 AND sex1_age < 94 THEN 16
					WHEN t1.SC_id_new=14 AND sex1_age >= 94 THEN 19
					WHEN t1.SC_id_new=14 THEN 10
					WHEN t1.SC_id_new IN(15) AND sex1_age = . THEN 9
					WHEN t1.SC_id_new IN(15) AND sex1_age < 32 THEN 5
					WHEN t1.SC_id_new IN(15) AND sex1_age >= 32 AND sex1_age < 64 THEN 9
					WHEN t1.SC_id_new IN(15) AND sex1_age >= 64 AND sex1_age < 84 THEN 12
					WHEN t1.SC_id_new IN(15) AND sex1_age >= 84 AND sex1_age < 94 THEN 14
					WHEN t1.SC_id_new IN(15) AND sex1_age >= 94 THEN 17
					WHEN t1.SC_id_new IN(15) THEN 9
					WHEN t1.SC_id_new=16 AND sex1_age = . THEN 6
					WHEN t1.SC_id_new=16 AND sex1_age < 57 THEN 2
					WHEN t1.SC_id_new=16 AND sex1_age >= 57 AND sex1_age < 104 THEN 6
					WHEN t1.SC_id_new=16 AND sex1_age >= 104 THEN 17
					WHEN t1.SC_id_new=16 THEN 6
					WHEN t1.SC_id_new IN(17) AND sex1_age = . THEN 13
					WHEN t1.SC_id_new IN(17) AND sex1_age < 57 THEN 2
					WHEN t1.SC_id_new IN(17) AND sex1_age >= 57 AND sex1_age < 104 THEN 8
					WHEN t1.SC_id_new IN(17) AND sex1_age >= 104 AND sex1_age < 114 THEN 13
					WHEN t1.SC_id_new IN(17) AND sex1_age >= 114 AND sex1_age < 124 THEN 16
					WHEN t1.SC_id_new IN(17) AND sex1_age >= 124 THEN 15
					WHEN t1.SC_id_new IN(17) THEN 13
				END) as w_sex_age_new,
				(CASE 
					WHEN t1.SC_ID_OLD=6 AND SPECIF = . THEN 14
					WHEN t1.SC_ID_OLD=6 AND SPECIF IN(10, 12, 13, 15, 24, 25, 26, 27, 29, 3, 30, 31, 32, 34, 36, 38, 39, 4, 40, 6, 7, 8) THEN 14
					WHEN t1.SC_ID_OLD=6 AND SPECIF IN(1, 11, 14, 16, 17, 18, 2, 20, 21, 22, 23, 35, 37) THEN 9
					WHEN t1.SC_ID_OLD=6 AND SPECIF IN(19, 28, 33, 9) THEN 6
					WHEN t1.SC_ID_OLD=6 THEN 14
					WHEN t1.SC_ID_OLD=7 AND SPECIF = . THEN 11
					WHEN t1.SC_ID_OLD=7 AND SPECIF IN(14, 16, 2, 24, 28, 35) THEN 10
					WHEN t1.SC_ID_OLD=7 AND SPECIF IN(1, 20, 21, 23, 33, 4, 7, 9, 10, 11, 13, 15, 17, 18, 22, 25, 26, 27, 37, 38, 8) THEN 11
					WHEN t1.SC_ID_OLD=7 AND SPECIF IN(12, 19, 29, 3, 30, 31, 32, 34, 36, 39, 6) THEN 12
					WHEN t1.SC_ID_OLD=7 THEN 11
					WHEN t1.SC_ID_OLD=8 AND SPECIF = . THEN 13
					WHEN t1.SC_ID_OLD=8 AND SPECIF IN(14, 16, 2, 20, 21, 23, 28, 33, 35, 9, 1, 10, 11, 15, 18, 22, 31, 37, 7) THEN 12
					WHEN t1.SC_ID_OLD=8 AND SPECIF IN(12, 17, 24, 25, 26, 27, 32, 38, 39, 8, 13, 19, 29, 3, 30, 34, 36, 4, 6) THEN 13
					WHEN t1.SC_ID_OLD=8 THEN 13
					WHEN t1.SC_ID_OLD=9 AND SPECIF = . THEN 12
					WHEN t1.SC_ID_OLD=9 AND SPECIF IN(12, 15, 17, 19, 26, 29, 3, 32, 34, 6, 7) THEN 16
					WHEN t1.SC_ID_OLD=9 AND SPECIF IN(13, 16, 27, 33, 36, 38, 4) THEN 12
					WHEN t1.SC_ID_OLD=9 AND SPECIF IN(1, 10, 14, 18, 20, 21, 22, 23, 24, 25, 30, 37, 8) THEN 9
					WHEN t1.SC_ID_OLD=9 AND SPECIF IN(11, 2, 28, 31, 35, 39, 9) THEN 7
					WHEN t1.SC_ID_OLD=9 THEN 12
					WHEN t1.SC_ID_OLD=10 AND SPECIF = . THEN 13
					WHEN t1.SC_ID_OLD=10 AND SPECIF IN(14, 16, 18, 20, 21, 23, 28, 37, 8, 9) THEN 9
					WHEN t1.SC_ID_OLD=10 AND SPECIF IN(1, 11, 17, 2, 22, 24, 25, 31, 32) THEN 13
					WHEN t1.SC_ID_OLD=10 AND SPECIF IN(10, 15, 26, 36, 38, 39, 7) THEN 14
					WHEN t1.SC_ID_OLD=10 AND SPECIF IN(12, 13, 19, 27, 29, 3, 30, 33, 34, 35, 4, 6) THEN 18
					WHEN t1.SC_ID_OLD=10 THEN 13
					WHEN t1.SC_ID_OLD=11 AND SPECIF = . THEN 11
					WHEN t1.SC_ID_OLD=11 AND SPECIF IN(11, 14, 17, 18, 2, 20, 21, 24, 33, 37, 4, 8) THEN 11
					WHEN t1.SC_ID_OLD=11 AND SPECIF IN(19, 22, 25, 26, 28, 34, 36, 38, 9) THEN 14
					WHEN t1.SC_ID_OLD=11 AND SPECIF IN(1, 10, 12, 13, 15, 16, 23, 27, 29, 3, 30, 31, 32, 35, 39, 6, 7) THEN 16
					WHEN t1.SC_ID_OLD=11 THEN 11
				END) AS w_specif,
				(CASE
					WHEN t1.SC_id_new=6 AND SPECIF = . THEN 14
					WHEN t1.SC_id_new=6 AND SPECIF IN(10, 12, 13, 15, 24, 25, 26, 27, 29, 3, 30, 31, 32, 34, 36, 38, 39, 4, 40, 6, 7, 8) THEN 14
					WHEN t1.SC_id_new=6 AND SPECIF IN(1, 11, 14, 16, 17, 18, 2, 20, 21, 22, 23, 35, 37) THEN 9
					WHEN t1.SC_id_new=6 AND SPECIF IN(19, 28, 33, 9) THEN 6
					WHEN t1.SC_id_new=6 THEN 14
					WHEN t1.SC_id_new=7 AND SPECIF = . THEN 11
					WHEN t1.SC_id_new=7 AND SPECIF IN(14, 16, 2, 24, 28, 35) THEN 10
					WHEN t1.SC_id_new=7 AND SPECIF IN(1, 20, 21, 23, 33, 4, 7, 9, 10, 11, 13, 15, 17, 18, 22, 25, 26, 27, 37, 38, 8) THEN 11
					WHEN t1.SC_id_new=7 AND SPECIF IN(12, 19, 29, 3, 30, 31, 32, 34, 36, 39, 6) THEN 12
					WHEN t1.SC_id_new=7 THEN 11
					WHEN t1.SC_id_new=8 AND SPECIF = . THEN 13
					WHEN t1.SC_id_new=8 AND SPECIF IN(14, 16, 2, 20, 21, 23, 28, 33, 35, 9, 1, 10, 11, 15, 18, 22, 31, 37, 7) THEN 12
					WHEN t1.SC_id_new=8 AND SPECIF IN(12, 17, 24, 25, 26, 27, 32, 38, 39, 8, 13, 19, 29, 3, 30, 34, 36, 4, 6) THEN 13
					WHEN t1.SC_id_new=8 THEN 13
					WHEN t1.SC_id_new=12 AND SPECIF = . THEN 12
					WHEN t1.SC_id_new=12 AND SPECIF IN(12, 15, 17, 19, 26, 29, 3, 32, 34, 6, 7) THEN 16
					WHEN t1.SC_id_new=12 AND SPECIF IN(13, 16, 27, 33, 36, 38, 4) THEN 12
					WHEN t1.SC_id_new=12 AND SPECIF IN(1, 10, 14, 18, 20, 21, 22, 23, 24, 25, 30, 37, 8) THEN 9
					WHEN t1.SC_id_new=12 AND SPECIF IN(11, 2, 28, 31, 35, 39, 9) THEN 7
					WHEN t1.SC_id_new=12 THEN 12
					WHEN t1.SC_id_new in(13) AND SPECIF = . THEN 8
					WHEN t1.SC_id_new in(13) AND SPECIF IN(2, 28, 30, 39) THEN 5
					WHEN t1.SC_id_new in(13) AND SPECIF IN(1, 11, 14, 18, 20, 21, 22, 25, 31, 33, 37, 4, 9) THEN 8
					WHEN t1.SC_id_new in(13) AND SPECIF IN(10, 13, 16, 23, 24, 27, 35, 36, 38, 7, 8) THEN 10
					WHEN t1.SC_id_new in(13) AND SPECIF IN(12, 15, 17, 19, 26, 29, 3, 32, 34, 6) THEN 14
					WHEN t1.SC_id_new in(13) THEN 8
					WHEN t1.SC_id_new=14 AND SPECIF = . THEN 13
					WHEN t1.SC_id_new=14 AND SPECIF IN(14, 16, 18, 20, 21, 23, 28, 37, 8, 9) THEN 9
					WHEN t1.SC_id_new=14 AND SPECIF IN(1, 11, 17, 2, 22, 24, 25, 31, 32) THEN 13
					WHEN t1.SC_id_new=14 AND SPECIF IN(10, 15, 26, 36, 38, 39, 7) THEN 14
					WHEN t1.SC_id_new=14 AND SPECIF IN(12, 13, 19, 27, 29, 3, 30, 33, 34, 35, 4, 6) THEN 18
					WHEN t1.SC_id_new=14 THEN 13
					WHEN t1.SC_id_new in(15) AND SPECIF = . THEN 15
					WHEN t1.SC_id_new in(15) AND SPECIF IN(14, 16, 2, 21, 28, 31, 37, 9) THEN 8
					WHEN t1.SC_id_new in(15) AND SPECIF IN(1, 11, 15, 17, 18, 20, 22, 23, 25, 39, 8) THEN 10
					WHEN t1.SC_id_new in(15) AND SPECIF IN(10, 12, 13, 19, 24, 26, 27, 29, 3, 30, 32, 33, 34, 35, 36, 38, 4, 6, 7) THEN 15
					WHEN t1.SC_id_new in(15) THEN 15
					WHEN t1.SC_id_new=16 AND SPECIF = . THEN 11
					WHEN t1.SC_id_new=16 AND SPECIF IN(11, 14, 17, 18, 2, 20, 21, 24, 33, 37, 4, 8) THEN 11
					WHEN t1.SC_id_new=16 AND SPECIF IN(19, 22, 25, 26, 28, 34, 36, 38, 9) THEN 14
					WHEN t1.SC_id_new=16 AND SPECIF IN(1, 10, 12, 13, 15, 16, 23, 27, 29, 3, 30, 31, 32, 35, 39, 6, 7) THEN 16
					WHEN t1.SC_id_new=16 THEN 11
					WHEN t1.SC_id_new in(17) AND SPECIF = . THEN 16
					WHEN t1.SC_id_new in(17) AND SPECIF IN(14, 18, 2, 20, 21, 24, 25, 28, 30, 37, 8) THEN 10
					WHEN t1.SC_id_new in(17) AND SPECIF IN(11, 13, 17, 19, 22, 23, 26, 27, 34, 35, 36, 38, 6, 9) THEN 13
					WHEN t1.SC_id_new in(17) AND SPECIF IN(1, 10, 12, 15, 16, 29, 3, 31, 32, 33, 39, 4, 7) THEN 16
					WHEN t1.SC_id_new in(17) THEN 16
				END) as w_specif_new,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=6 AND stag=. THEN 17
					WHEN t1.SC_ID_OLD=6 AND stag< 4 THEN 9
					WHEN t1.SC_ID_OLD=6 AND stag>= 4 AND stag< 5 THEN 12
					WHEN t1.SC_ID_OLD=6 AND stag>= 5 THEN 17
					WHEN t1.SC_ID_OLD=6 THEN 17
					WHEN t1.SC_ID_OLD=7 AND stag=. THEN 13
					WHEN t1.SC_ID_OLD=7 AND stag< 2 THEN 13
					WHEN t1.SC_ID_OLD=7 AND stag>= 2 AND stag< 4 THEN 7
					WHEN t1.SC_ID_OLD=7 AND stag>= 4 AND stag< 5 THEN 10
					WHEN t1.SC_ID_OLD=7 AND stag>= 5 AND stag< 6 THEN 13
					WHEN t1.SC_ID_OLD=7 AND stag>= 6 THEN 17
					WHEN t1.SC_ID_OLD=7 THEN 13
					WHEN t1.SC_ID_OLD=8 AND stag=. THEN 8
					WHEN t1.SC_ID_OLD=8 AND stag< 2 THEN 14
					WHEN t1.SC_ID_OLD=8 AND stag>= 2 AND stag< 4 THEN 8
					WHEN t1.SC_ID_OLD=8 AND stag>= 4 AND stag< 5 THEN 12
					WHEN t1.SC_ID_OLD=8 AND stag>= 5 THEN 19
					WHEN t1.SC_ID_OLD=8 THEN 8
					WHEN t1.SC_ID_OLD=9 AND stag=. THEN 7
					WHEN t1.SC_ID_OLD=9 AND stag< 2 THEN 12
					WHEN t1.SC_ID_OLD=9 AND stag>= 2 AND stag < 4  THEN 7
					WHEN t1.SC_ID_OLD=9 AND stag>= 4 AND stag < 5  THEN 10
					WHEN t1.SC_ID_OLD=9 AND stag>= 5 THEN 14
					WHEN t1.SC_ID_OLD=9 THEN 7
					WHEN t1.SC_ID_OLD=10 AND stag = . THEN 13
					WHEN t1.SC_ID_OLD=10 AND stag< 4 THEN 9
					WHEN t1.SC_ID_OLD=10 AND stag>= 4 AND stag < 5  THEN 13
					WHEN t1.SC_ID_OLD=10 AND stag>= 5 AND stag < 6  THEN 14
					WHEN t1.SC_ID_OLD=10 AND stag>= 6 THEN 18
					WHEN t1.SC_ID_OLD=10 THEN 13
					WHEN t1.SC_ID_OLD=11 AND stag = . THEN 11
					WHEN t1.SC_ID_OLD=11 AND stag< 3 THEN 13
					WHEN t1.SC_ID_OLD=11 AND stag>= 3 AND stag < 5  THEN 11
					WHEN t1.SC_ID_OLD=11 AND stag>= 5 AND stag < 6  THEN 14
					WHEN t1.SC_ID_OLD=11 AND stag>= 6 THEN 18
					WHEN t1.SC_ID_OLD=11 THEN 11
					END) AS w_stag,
				(CASE 
					WHEN t1.SC_id_new=6 AND stag=. THEN 17
					WHEN t1.SC_id_new=6 AND stag< 4 THEN 9
					WHEN t1.SC_id_new=6 AND stag>= 4 AND stag< 5 THEN 12
					WHEN t1.SC_id_new=6 AND stag>= 5 THEN 17
					WHEN t1.SC_id_new=6 THEN 17
					WHEN t1.SC_id_new=7 AND stag=. THEN 13
					WHEN t1.SC_id_new=7 AND stag< 2 THEN 13
					WHEN t1.SC_id_new=7 AND stag>= 2 AND stag< 4 THEN 7
					WHEN t1.SC_id_new=7 AND stag>= 4 AND stag< 5 THEN 10
					WHEN t1.SC_id_new=7 AND stag>= 5 AND stag< 6 THEN 13
					WHEN t1.SC_id_new=7 AND stag>= 6 THEN 17
					WHEN t1.SC_id_new=7 THEN 13
					WHEN t1.SC_id_new=8 AND stag=. THEN 8
					WHEN t1.SC_id_new=8 AND stag< 2 THEN 14
					WHEN t1.SC_id_new=8 AND stag>= 2 AND stag< 4 THEN 8
					WHEN t1.SC_id_new=8 AND stag>= 4 AND stag< 5 THEN 12
					WHEN t1.SC_id_new=8 AND stag>= 5 THEN 19
					WHEN t1.SC_id_new=8 THEN 8
					WHEN t1.SC_id_new=12 AND stag=. THEN 7
					WHEN t1.SC_id_new=12 AND stag< 2 THEN 12
					WHEN t1.SC_id_new=12 AND stag>= 2 AND stag < 4  THEN 7
					WHEN t1.SC_id_new=12 AND stag>= 4 AND stag < 5  THEN 10
					WHEN t1.SC_id_new=12 AND stag>= 5 THEN 14
					WHEN t1.SC_id_new=12 THEN 7
					WHEN t1.SC_id_new in(13) AND stag=. THEN 15
					WHEN t1.SC_id_new in(13) AND stag IN(1) THEN 12
					WHEN t1.SC_id_new in(13) AND stag IN(2,3) THEN 6
					WHEN t1.SC_id_new in(13) AND stag IN(4) THEN 9
					WHEN t1.SC_id_new in(13) AND stag IN(5,6) THEN 15
					WHEN t1.SC_id_new in(13) THEN 15
					WHEN t1.SC_id_new=14 AND stag = . THEN 13
					WHEN t1.SC_id_new=14 AND stag< 4 THEN 9
					WHEN t1.SC_id_new=14 AND stag>= 4 AND stag < 5  THEN 13
					WHEN t1.SC_id_new=14 AND stag>= 5 AND stag < 6  THEN 14
					WHEN t1.SC_id_new=14 AND stag>= 6 THEN 18
					WHEN t1.SC_id_new=14 THEN 13
					WHEN t1.SC_id_new in(15) AND stag=. THEN 18
					WHEN t1.SC_id_new in(15) AND stag IN(2,3) THEN 8
					WHEN t1.SC_id_new in(15) AND stag IN(1,4) THEN 10
					WHEN t1.SC_id_new in(15) AND stag IN(5) THEN 14
					WHEN t1.SC_id_new in(15) AND stag IN(6) THEN 18
					WHEN t1.SC_id_new in(15) THEN 18
					WHEN t1.SC_id_new=16 AND stag = . THEN 11
					WHEN t1.SC_id_new=16 AND stag< 3 THEN 13
					WHEN t1.SC_id_new=16 AND stag>= 3 AND stag < 5  THEN 11
					WHEN t1.SC_id_new=16 AND stag>= 5 AND stag < 6  THEN 14
					WHEN t1.SC_id_new=16 AND stag>= 6 THEN 18
					WHEN t1.SC_id_new=16 THEN 11
					WHEN t1.SC_id_new in(17) AND stag=. THEN 9
					WHEN t1.SC_id_new in(17) AND stag IN(2,3) THEN 9
					WHEN t1.SC_id_new in(17) AND stag IN(4) THEN 11
					WHEN t1.SC_id_new in(17) AND stag IN(1,5) THEN 13
					WHEN t1.SC_id_new in(17) AND stag IN(6) THEN 16
					WHEN t1.SC_id_new in(17) THEN 9
				END) as w_stag_new,
					/*input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2) as autofilters_new,*/
					
 				/*(CASE 
					/* correct else */
					/*WHEN SC_ID_OLD=6 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2) = . THEN 5
					WHEN SC_ID_OLD=6 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -1.35 THEN 9
					WHEN SC_ID_OLD=6 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -1.35 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< 0 THEN 16
					WHEN SC_ID_OLD=6 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= 0 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< 0.14 THEN 10
					WHEN SC_ID_OLD=6 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= 0.14 THEN 5
					WHEN SC_ID_OLD=6 THEN 5
					WHEN SC_ID_OLD=7 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2) = . THEN 10
					WHEN SC_ID_OLD=7 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< 0 THEN 12
					WHEN SC_ID_OLD=7 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= 0 THEN 10
					WHEN SC_ID_OLD=7 THEN 10
					WHEN SC_ID_OLD=8 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2) = . THEN 9
					WHEN SC_ID_OLD=8 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -1.35 THEN 9
					WHEN SC_ID_OLD=8 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -1.35 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< 0 THEN 18
					WHEN SC_ID_OLD=8 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= 0 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< 0.3 THEN 13
					WHEN SC_ID_OLD=8 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= 0.3 THEN 1
					WHEN SC_ID_OLD=8 THEN 9
					WHEN SC_ID_OLD=9 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2) = . THEN 11
					WHEN SC_ID_OLD=9 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -1.35 THEN 5
					WHEN SC_ID_OLD=9 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -1.35 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -1.2 THEN 11
					WHEN SC_ID_OLD=9 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -1.2 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -0.75 THEN 13
					WHEN SC_ID_OLD=9 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -0.75 THEN 10
					WHEN SC_ID_OLD=9 THEN 11
					WHEN SC_ID_OLD=10 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2) = . THEN 6
					WHEN SC_ID_OLD=10 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -1.28 THEN 6
					WHEN SC_ID_OLD=10 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -1.28 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -0.21 THEN 15
					WHEN SC_ID_OLD=10 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -0.21 THEN 6
					WHEN SC_ID_OLD=10 THEN 6
					WHEN SC_ID_OLD=11 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2) = . THEN 7
					WHEN SC_ID_OLD=11 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -1.28 THEN 7
					WHEN SC_ID_OLD=11 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -1.28 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -0.9 THEN 15
					WHEN SC_ID_OLD=11 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -0.9 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)< -0.5 THEN 18
					WHEN SC_ID_OLD=11 AND input((tranwrd(tranwrd(substr(oprosnik, 4, index(oprosnik,'/')-4), "/3>",""), ",", ".")), 5.2)>= -0.5 THEN 6
					WHEN SC_ID_OLD=11 THEN 7
				END) AS w_autofilters_opr,*/
				 (CASE 
					/* correct else */
					WHEN SC_ID_OLD=6 AND auto_weight = . THEN 5
					WHEN SC_ID_OLD=6 AND auto_weight< -1.35 THEN 9
					WHEN SC_ID_OLD=6 AND auto_weight>= -1.35 AND auto_weight< 0 THEN 16
					WHEN SC_ID_OLD=6 AND auto_weight>= 0 AND auto_weight< 0.14 THEN 10
					WHEN SC_ID_OLD=6 AND auto_weight>= 0.14 THEN 5
					WHEN SC_ID_OLD=6 THEN 5
					WHEN SC_ID_OLD=7 AND auto_weight = . THEN 10
					WHEN SC_ID_OLD=7 AND auto_weight< 0 THEN 12
					WHEN SC_ID_OLD=7 AND auto_weight>= 0 THEN 10
					WHEN SC_ID_OLD=7 THEN 10
					WHEN SC_ID_OLD=8 AND auto_weight = . THEN 9
					WHEN SC_ID_OLD=8 AND auto_weight< -1.35 THEN 9
					WHEN SC_ID_OLD=8 AND auto_weight>= -1.35 AND auto_weight< 0 THEN 18
					WHEN SC_ID_OLD=8 AND auto_weight>= 0 AND auto_weight< 0.3 THEN 13
					WHEN SC_ID_OLD=8 AND auto_weight>= 0.3 THEN 1
					WHEN SC_ID_OLD=8 THEN 9
					WHEN SC_ID_OLD=9 AND auto_weight = . THEN 11
					WHEN SC_ID_OLD=9 AND auto_weight< -1.35 THEN 5
					WHEN SC_ID_OLD=9 AND auto_weight>= -1.35 AND auto_weight< -1.2 THEN 11
					WHEN SC_ID_OLD=9 AND auto_weight>= -1.2 AND auto_weight< -0.75 THEN 13
					WHEN SC_ID_OLD=9 AND auto_weight>= -0.75 THEN 10
					WHEN SC_ID_OLD=9 THEN 11
					WHEN SC_ID_OLD=10 AND auto_weight = . THEN 6
					WHEN SC_ID_OLD=10 AND auto_weight< -1.28 THEN 6
					WHEN SC_ID_OLD=10 AND auto_weight>= -1.28 AND auto_weight< -0.21 THEN 15
					WHEN SC_ID_OLD=10 AND auto_weight>= -0.21 THEN 6
					WHEN SC_ID_OLD=10 THEN 6
					WHEN SC_ID_OLD=11 AND auto_weight = . THEN 7
					WHEN SC_ID_OLD=11 AND auto_weight< -1.28 THEN 7
					WHEN SC_ID_OLD=11 AND auto_weight>= -1.28 AND auto_weight< -0.9 THEN 15
					WHEN SC_ID_OLD=11 AND auto_weight>= -0.9 AND auto_weight< -0.5 THEN 18
					WHEN SC_ID_OLD=11 AND auto_weight>= -0.5 THEN 6
					WHEN SC_ID_OLD=11 THEN 7
				END) AS w_autofilters,
				(CASE
					WHEN t1.SC_id_new=6 AND auto_weight = . THEN 5
					WHEN t1.SC_id_new=6 AND auto_weight< -1.35 THEN 9
					WHEN t1.SC_id_new=6 AND auto_weight>= -1.35 AND auto_weight< 0 THEN 16
					WHEN t1.SC_id_new=6 AND auto_weight>= 0 AND auto_weight< 0.14 THEN 10
					WHEN t1.SC_id_new=6 AND auto_weight>= 0.14 THEN 5
					WHEN t1.SC_id_new=6 THEN 5
					WHEN t1.SC_id_new=7 AND auto_weight = . THEN 10
					WHEN t1.SC_id_new=7 AND auto_weight< 0 THEN 12
					WHEN t1.SC_id_new=7 AND auto_weight>= 0 THEN 10
					WHEN t1.SC_id_new=7 THEN 10
					WHEN t1.SC_id_new=8 AND auto_weight = . THEN 9
					WHEN t1.SC_id_new=8 AND auto_weight< -1.35 THEN 9
					WHEN t1.SC_id_new=8 AND auto_weight>= -1.35 AND auto_weight< 0 THEN 18
					WHEN t1.SC_id_new=8 AND auto_weight>= 0 AND auto_weight< 0.3 THEN 13
					WHEN t1.SC_id_new=8 AND auto_weight>= 0.3 THEN 1
					WHEN t1.SC_id_new=8 THEN 9
					WHEN SC_id_new=12 AND auto_weight = . THEN 11
					WHEN SC_id_new=12 AND auto_weight< -1.35 THEN 5
					WHEN SC_id_new=12 AND auto_weight>= -1.35 AND auto_weight< -1.2 THEN 11
					WHEN SC_id_new=12 AND auto_weight>= -1.2 AND auto_weight< -0.75 THEN 13
					WHEN SC_id_new=12 AND auto_weight>= -0.75 THEN 10
					WHEN SC_id_new=12 THEN 11
					WHEN t1.SC_id_new in(13) AND auto_weight = . THEN 3
					WHEN t1.SC_id_new in(13) AND auto_weight< -1.35 THEN 3
					WHEN t1.SC_id_new in(13) AND auto_weight>= -1.35 AND auto_weight< -0.83 THEN 13
					WHEN t1.SC_id_new in(13) AND auto_weight>= -0.83 AND auto_weight< -0.6 THEN 15
					WHEN t1.SC_id_new in(13) AND auto_weight>= -0.6 THEN 4
					WHEN t1.SC_id_new in(13) THEN 3
					WHEN SC_id_new=14 AND auto_weight = . THEN 6
					WHEN SC_id_new=14 AND auto_weight< -1.28 THEN 6
					WHEN SC_id_new=14 AND auto_weight>= -1.28 AND auto_weight< -0.21 THEN 15
					WHEN SC_id_new=14 AND auto_weight>= -0.21 THEN 6
					WHEN SC_id_new=14 THEN 6
					WHEN t1.SC_id_new in(15) AND auto_weight = . THEN 5
					WHEN t1.SC_id_new in(15) AND auto_weight< -1.2 THEN 5
					WHEN t1.SC_id_new in(15) AND auto_weight>= -1.2 AND auto_weight< -0.83 THEN 13
					WHEN t1.SC_id_new in(15) AND auto_weight>= -1.2 AND auto_weight< -0.55 THEN 16
					WHEN t1.SC_id_new in(15) AND auto_weight>= -0.55 THEN 5
					WHEN t1.SC_id_new in(15) THEN 5
					WHEN SC_id_new=16 AND auto_weight = . THEN 7
					WHEN SC_id_new=16 AND auto_weight< -1.28 THEN 7
					WHEN SC_id_new=16 AND auto_weight>= -1.28 AND auto_weight< -0.9 THEN 15
					WHEN SC_id_new=16 AND auto_weight>= -0.9 AND auto_weight< -0.5 THEN 18
					WHEN SC_id_new=16 AND auto_weight>= -0.5 THEN 6
					WHEN SC_id_new=16 THEN 7
					WHEN t1.SC_id_new in(17) AND auto_weight = . THEN 3
					WHEN t1.SC_id_new in(17) AND auto_weight< -1.2 THEN 7
					WHEN t1.SC_id_new in(17) AND auto_weight>= -1.2 AND auto_weight< -0.9 THEN 15
					WHEN t1.SC_id_new in(17) AND auto_weight>= -0.9 AND auto_weight< -0.7 THEN 17
					WHEN t1.SC_id_new in(17) AND auto_weight>= -0.7 THEN 3
					WHEN t1.SC_id_new in(17) THEN 3
				END) as w_autofilters_new,
				(CASE
					WHEN t1.SC_id_new in(14,15) AND dd1chs = . THEN 13
					WHEN t1.SC_id_new in(14,15) AND dd1chs< 1354.33 THEN 7
					WHEN t1.SC_id_new in(14,15) AND dd1chs>= 1354.33 AND dd1chs< 6257.67 THEN 10
					WHEN t1.SC_id_new in(14,15) AND dd1chs>= 6257.67 AND dd1chs< 28662.33 THEN 13
					WHEN t1.SC_id_new in(14,15) AND dd1chs>= 28662.33 THEN 9
					WHEN t1.SC_id_new in(14,15) THEN 13
					WHEN t1.SC_id_new in(16,17) AND dd1chs = . THEN 13
					WHEN t1.SC_id_new in(16,17) AND dd1chs< 5930.5 THEN 10
					WHEN t1.SC_id_new in(16,17) AND dd1chs>= 5930.5 AND dd1chs< 16158.33 THEN 13
					WHEN t1.SC_id_new in(16,17) AND dd1chs>= 16158.33 AND dd1chs< 30592 THEN 16
					WHEN t1.SC_id_new in(16,17) AND dd1chs>= 30592 THEN 9
					WHEN t1.SC_id_new in(16,17) THEN 13
				END) as w_dd1chs,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=6 AND CHANNEL = . THEN 13
					WHEN t1.SC_ID_OLD=6 AND CHANNEL = 2 THEN 1
					WHEN t1.SC_ID_OLD=6 AND CHANNEL = 1 THEN 13
					WHEN t1.SC_ID_OLD=6 THEN 13
					WHEN t1.SC_ID_OLD=7 AND CHANNEL = . THEN 14
					WHEN t1.SC_ID_OLD=7 AND CHANNEL = 2 THEN 2
					WHEN t1.SC_ID_OLD=7 AND CHANNEL = 1 THEN 14
					WHEN t1.SC_ID_OLD=7 THEN 14
					WHEN t1.SC_ID_OLD=8 AND CHANNEL = . THEN 15
					WHEN t1.SC_ID_OLD=8 AND CHANNEL = 2 THEN 1
					WHEN t1.SC_ID_OLD=8 AND CHANNEL IN(0.5, 1) THEN 15
					WHEN t1.SC_ID_OLD=8 THEN 15
					WHEN t1.SC_ID_OLD=9 AND CHANNEL = . THEN 13
					WHEN t1.SC_ID_OLD=9 AND CHANNEL = 2 THEN 5
					WHEN t1.SC_ID_OLD=9 AND CHANNEL IN(0.5, 1) THEN 13
					WHEN t1.SC_ID_OLD=9 THEN 13
					WHEN t1.SC_ID_OLD=10 AND CHANNEL = . THEN 14
					WHEN t1.SC_ID_OLD=10 AND CHANNEL = 1 THEN 14
					WHEN t1.SC_ID_OLD=10 AND CHANNEL IN(0.5, 2) THEN 5
					WHEN t1.SC_ID_OLD=10 THEN 14
				END) AS w_channel_groups,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=7 AND d = . THEN 11
					WHEN t1.SC_ID_OLD=7 AND d < 0.1 THEN 13
					WHEN t1.SC_ID_OLD=7 AND d >= 0.1 AND D < 0.13 THEN 12
					WHEN t1.SC_ID_OLD=7 AND d >= 0.13 AND D < 0.17 THEN 11
					WHEN t1.SC_ID_OLD=7 AND d >= 0.17 THEN 9
					WHEN t1.SC_ID_OLD=7 THEN 11
					WHEN t1.SC_ID_OLD=8 AND d = . THEN 13
					WHEN t1.SC_ID_OLD=8 AND d < 0.1 THEN 15
					WHEN t1.SC_ID_OLD=8 AND d >= 0.1 AND d< 0.15 THEN 13
					WHEN t1.SC_ID_OLD=8 AND d >= 0.15 AND d< 0.18 THEN 11
					WHEN t1.SC_ID_OLD=8 AND d >= 0.18 THEN 8
					WHEN t1.SC_ID_OLD=8 THEN 13
					WHEN t1.SC_ID_OLD=11 AND d = . THEN 12
					WHEN t1.SC_ID_OLD=11 AND d < 0.05 THEN 16
					WHEN t1.SC_ID_OLD=11 AND d >= 0.05 AND d < 0.12 THEN 12
					WHEN t1.SC_ID_OLD=11 AND d >= 0.12 THEN 10
					WHEN t1.SC_ID_OLD=11 THEN 12
				END) AS w_d,
				(CASE 
					WHEN t1.SC_id_new=7 AND d = . THEN 11
					WHEN t1.SC_id_new=7 AND d < 0.1 THEN 13
					WHEN t1.SC_id_new=7 AND d >= 0.1 AND D < 0.13 THEN 12
					WHEN t1.SC_id_new=7 AND d >= 0.13 AND D < 0.17 THEN 11
					WHEN t1.SC_id_new=7 AND d >= 0.17 THEN 9
					WHEN t1.SC_id_new=7 THEN 11
					WHEN t1.SC_id_new=8 AND d = . THEN 13
					WHEN t1.SC_id_new=8 AND d < 0.1 THEN 15
					WHEN t1.SC_id_new=8 AND d >= 0.1 AND d< 0.15 THEN 13
					WHEN t1.SC_id_new=8 AND d >= 0.15 AND d< 0.18 THEN 11
					WHEN t1.SC_id_new=8 AND d >= 0.18 THEN 8
					WHEN t1.SC_id_new=8 THEN 13
					WHEN t1.SC_id_new=16 AND d = . THEN 12
					WHEN t1.SC_id_new=16 AND d < 0.05 THEN 16
					WHEN t1.SC_id_new=16 AND d >= 0.05 AND d < 0.12 THEN 12
					WHEN t1.SC_id_new=16 AND d >= 0.12 THEN 10
					WHEN t1.SC_id_new=16 THEN 12
					WHEN t1.SC_id_new IN(17) AND d = . THEN 14
					WHEN t1.SC_id_new IN(17) AND d < 0.05 THEN 14
					WHEN t1.SC_id_new IN(17) AND d >= 0.05 AND d < 0.1 THEN 12
					WHEN t1.SC_id_new IN(17) AND d >= 0.1 AND d < 0.12 THEN 11
					WHEN t1.SC_id_new IN(17) AND d >= 0.12 THEN 9
					WHEN t1.SC_id_new IN(17) THEN 12
				END) as w_d_new,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=7 AND bnk_group = . THEN 2
					WHEN t1.SC_ID_OLD=7 AND bnk_group = 1 THEN 2
					WHEN t1.SC_ID_OLD=7 AND bnk_group = 2 THEN 2
					WHEN t1.SC_ID_OLD=7 AND bnk_group = 3 THEN 13
					WHEN t1.SC_ID_OLD=7 THEN 2
					WHEN t1.SC_ID_OLD=8 AND bnk_group = . THEN 14
					WHEN t1.SC_ID_OLD=8 AND bnk_group = 1 THEN 4
					WHEN t1.SC_ID_OLD=8 AND bnk_group = 2 THEN 4
					WHEN t1.SC_ID_OLD=8 AND bnk_group = 3 THEN 14
					WHEN t1.SC_ID_OLD=8 THEN 14
				END) AS w_bnk_type,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=9 AND age = . THEN 8
					WHEN t1.SC_ID_OLD=9 AND age < 32 THEN 5
					WHEN t1.SC_ID_OLD=9 AND age >= 32 AND age < 42 THEN 8
					WHEN t1.SC_ID_OLD=9 AND age >= 42 AND age < 52 THEN 12
					WHEN t1.SC_ID_OLD=9 AND age >= 52 THEN 17
					WHEN t1.SC_ID_OLD=9 THEN 8
				END) AS w_age,
				(CASE 
					/* correct else */
					WHEN t1.SC_id_new=12 AND age = . THEN 8
					WHEN t1.SC_id_new=12 AND age < 32 THEN 5
					WHEN t1.SC_id_new=12 AND age >= 32 AND age < 42 THEN 8
					WHEN t1.SC_id_new=12 AND age >= 42 AND age < 52 THEN 12
					WHEN t1.SC_id_new=12 AND age >= 52 THEN 17
					WHEN t1.SC_id_new=12 THEN 8
					WHEN t1.SC_id_new in(13) AND age = . THEN 11
					WHEN t1.SC_id_new in(13) AND age < 27 THEN 5
					WHEN t1.SC_id_new in(13) AND age >= 27 AND age < 32 THEN 7
					WHEN t1.SC_id_new in(13) AND age >= 32 AND age < 42 THEN 8
					WHEN t1.SC_id_new in(13) AND age >= 42 AND age < 57 THEN 11
					WHEN t1.SC_id_new in(13) AND age >= 57 THEN 13
					WHEN t1.SC_id_new in(13) THEN 11
				END) AS w_age_new,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=9 AND count_kredit_debt = . THEN 27
					WHEN t1.SC_ID_OLD=9 AND count_kredit_debt< 1 THEN -9 
					WHEN t1.SC_ID_OLD=9 AND count_kredit_debt>= 1 AND count_kredit_debt< 2 THEN 27
					WHEN t1.SC_ID_OLD=9 AND count_kredit_debt>= 2 THEN -3
					WHEN t1.SC_ID_OLD=9 THEN 27
					WHEN t1.SC_ID_OLD=10 AND count_kredit_debt = . THEN 29
					WHEN t1.SC_ID_OLD=10 AND count_kredit_debt< 1 THEN -2 
					WHEN t1.SC_ID_OLD=10 AND count_kredit_debt>= 1 AND count_kredit_debt< 2 THEN 29
					WHEN t1.SC_ID_OLD=10 AND count_kredit_debt>= 2 AND count_kredit_debt< 3 THEN 11
					WHEN t1.SC_ID_OLD=10 AND count_kredit_debt>= 3 THEN 2
					WHEN t1.SC_ID_OLD=10 THEN 29
					WHEN t1.SC_ID_OLD=11 AND count_kredit_debt = . THEN 31
					WHEN t1.SC_ID_OLD=11 AND count_kredit_debt< 1 THEN 0 
					WHEN t1.SC_ID_OLD=11 AND count_kredit_debt>= 1 AND count_kredit_debt< 2 THEN 31
					WHEN t1.SC_ID_OLD=11 AND count_kredit_debt>= 2 AND count_kredit_debt< 3 THEN 14
					WHEN t1.SC_ID_OLD=11 AND count_kredit_debt>= 3 THEN -2
					WHEN t1.SC_ID_OLD=11 THEN 31
				END) AS w_count_kredit_debt,
				(CASE
					WHEN t1.SC_id_new IN(12) AND cnt_credit_veb = . THEN 27
					WHEN t1.SC_id_new IN(12) AND cnt_credit_veb< 1 THEN -9 
					WHEN t1.SC_id_new IN(12)AND cnt_credit_veb>= 1 AND cnt_credit_veb< 2 THEN 27
					WHEN t1.SC_id_new IN(12) AND cnt_credit_veb>= 2 THEN -3
					WHEN t1.SC_id_new IN(12) THEN 27
					WHEN t1.SC_id_new IN(13) AND cnt_credit_veb = . THEN 27
					WHEN t1.SC_id_new IN(13) AND cnt_credit_veb< 1 THEN -9 
					WHEN t1.SC_id_new IN(13) AND cnt_credit_veb>= 1 AND cnt_credit_veb< 2 THEN 27
					WHEN t1.SC_id_new IN(13) AND cnt_credit_veb>= 2 THEN -3
					WHEN t1.SC_id_new IN(13) THEN 27
					WHEN t1.SC_id_new in(14) AND cnt_credit_veb = . THEN 29
					WHEN t1.SC_id_new in(14) AND cnt_credit_veb< 1 THEN -2 
					WHEN t1.SC_id_new in(14) AND cnt_credit_veb>= 1 AND cnt_credit_veb< 2 THEN 29
					WHEN t1.SC_id_new in(14) AND cnt_credit_veb>= 2 AND cnt_credit_veb< 3 THEN 11
					WHEN t1.SC_id_new in(14) AND cnt_credit_veb>= 3 THEN 2
					WHEN t1.SC_id_new in(14) THEN 29
					WHEN t1.SC_id_new in(15) AND cnt_credit_veb = . THEN 23
					WHEN t1.SC_id_new in(15) AND cnt_credit_veb< 1 THEN -2
					WHEN t1.SC_id_new in(15) AND cnt_credit_veb>= 1 AND cnt_credit_veb< 2 THEN 23
					WHEN t1.SC_id_new in(15) AND cnt_credit_veb>= 2 AND cnt_credit_veb< 3 THEN 12
					WHEN t1.SC_id_new in(15) AND cnt_credit_veb>= 3 THEN 0
					WHEN t1.SC_id_new in(15) THEN 23
					WHEN t1.SC_id_new IN(16) AND cnt_credit_veb = . THEN 31
					WHEN t1.SC_id_new IN(16) AND cnt_credit_veb< 1 THEN 0 
					WHEN t1.SC_id_new IN(16) AND cnt_credit_veb>= 1 AND cnt_credit_veb< 2 THEN 31
					WHEN t1.SC_id_new IN(16) AND cnt_credit_veb>= 2 AND cnt_credit_veb< 3 THEN 14
					WHEN t1.SC_id_new IN(16) AND cnt_credit_veb>= 3 THEN -2
					WHEN t1.SC_id_new IN(16) THEN 31
					WHEN t1.SC_id_new IN(17) AND cnt_credit_veb = . THEN 31
					WHEN t1.SC_id_new IN(17) AND cnt_credit_veb< 1 THEN 0 
					WHEN t1.SC_id_new IN(17) AND cnt_credit_veb>= 1 AND cnt_credit_veb< 2 THEN 31
					WHEN t1.SC_id_new IN(17) AND cnt_credit_veb>= 2 AND cnt_credit_veb< 3 THEN 14
					WHEN t1.SC_id_new IN(17) AND cnt_credit_veb>= 3 THEN -2
					WHEN t1.SC_id_new IN(17) THEN 31
				END) as w_cnt_credit_veb,
				(CASE 
					/* correct else */
					/* в данном случае условие [od_sum_veb = . ] не обязательно, но следует оставить, если следующее значение будет другим */
					WHEN t1.SC_ID_OLD=9 AND od_sum_veb = . THEN 12 
					WHEN t1.SC_ID_OLD=9 AND od_sum_veb< 0.3 THEN 12
					WHEN t1.SC_ID_OLD=9 AND od_sum_veb>= 0.3 AND od_sum_veb< 1 THEN 14
					WHEN t1.SC_ID_OLD=9 AND od_sum_veb>= 1 THEN -2
					WHEN t1.SC_ID_OLD=9 THEN 12
					WHEN t1.SC_ID_OLD=10 AND od_sum_veb = . THEN 14
					WHEN t1.SC_ID_OLD=10 AND od_sum_veb< 0.5 THEN 14
					WHEN t1.SC_ID_OLD=10 AND od_sum_veb>= 0.5 AND od_sum_veb< 0.8 THEN 23
					WHEN t1.SC_ID_OLD=10 AND od_sum_veb>= 0.8 THEN 2
					WHEN t1.SC_ID_OLD=10 THEN 14
					WHEN t1.SC_ID_OLD=11 AND od_sum_veb = . THEN 16
					WHEN t1.SC_ID_OLD=11 AND od_sum_veb< 0.3 THEN 16
					WHEN t1.SC_ID_OLD=11 AND od_sum_veb>= 0.3 AND od_sum_veb< 0.7 THEN 27
					WHEN t1.SC_ID_OLD=11 AND od_sum_veb>= 0.7 AND od_sum_veb< 0.9 THEN 10
					WHEN t1.SC_ID_OLD=11 AND od_sum_veb>= 0.9 THEN -1
					WHEN t1.SC_ID_OLD=11 THEN 16
				END) AS w_od_sum_veb,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=9 AND antiquity_veb = . THEN 8
					WHEN t1.SC_ID_OLD=9 AND antiquity_veb < 1 THEN 2
					WHEN t1.SC_ID_OLD=9 AND antiquity_veb >= 1 AND antiquity_veb< 2 THEN 8
					WHEN t1.SC_ID_OLD=9 AND antiquity_veb >= 2 AND antiquity_veb< 4 THEN 13
					WHEN t1.SC_ID_OLD=9 AND antiquity_veb >= 4 THEN 42
					WHEN t1.SC_ID_OLD=9 THEN 8
				END) AS w_antiquity_veb,
				(CASE 
					/* correct else */
					WHEN t1.SC_id_new=12 AND antiquity_veb = . THEN 8
					WHEN t1.SC_id_new=12 AND antiquity_veb < 1 THEN 2
					WHEN t1.SC_id_new=12 AND antiquity_veb >= 1 AND antiquity_veb< 2 THEN 8
					WHEN t1.SC_id_new=12 AND antiquity_veb >= 2 AND antiquity_veb< 4 THEN 13
					WHEN t1.SC_id_new=12 AND antiquity_veb >= 4 THEN 42
					WHEN t1.SC_id_new=12 THEN 8
					WHEN t1.SC_id_new IN(13) AND antiquity_veb = . THEN 9
					WHEN t1.SC_id_new IN(13) AND antiquity_veb < 1 THEN 6
					WHEN t1.SC_id_new IN(13) AND antiquity_veb >= 1 AND antiquity_veb< 2 THEN 9
					WHEN t1.SC_id_new IN(13) AND antiquity_veb >= 2 AND antiquity_veb< 3 THEN 11
					WHEN t1.SC_id_new IN(13) AND antiquity_veb >= 3 THEN 20
					WHEN t1.SC_id_new IN(13) THEN 9
				END) AS w_antiquity_veb_new,
				(CASE 
					/* correct else */
					WHEN t1.SC_ID_OLD=11 AND cities_type = . THEN 10
					WHEN t1.SC_ID_OLD=11 AND cities_type IN(3, 4, 5, 7) THEN 10
					WHEN t1.SC_ID_OLD=11 AND cities_type IN(1, 2, 6, 8) THEN 15
					WHEN t1.SC_ID_OLD=11 THEN 10
				END) AS w_cities_type,
				(CASE 
					/* correct. Дублирует последнюю проверку. Для исправления ошибки проверки при преобразовании данных.*/
					WHEN t1.SC_ID_OLD=11 and l_ki_veb = . THEN 18 
					WHEN t1.SC_ID_OLD=11 AND l_ki_veb< 20 THEN 7
					WHEN t1.SC_ID_OLD=11 AND l_ki_veb>= 20 AND l_ki_veb< 34 THEN 11
					WHEN t1.SC_ID_OLD=11 AND l_ki_veb>= 34 THEN 18
					WHEN t1.SC_ID_OLD=11 THEN 18
				END) AS w_l_ki_veb,
				(CASE 
					/* correct. Дублирует последнюю проверку. Для исправления ошибки проверки при преобразовании данных.*/
					WHEN t1.SC_id_new IN(14,15) and l_ki_veb = . THEN 18 
					WHEN t1.SC_id_new IN(14,15) AND l_ki_veb< 16 THEN 5
					WHEN t1.SC_id_new IN(14,15) AND l_ki_veb>= 16 AND l_ki_veb< 24 THEN 7
					WHEN t1.SC_id_new IN(14,15) AND l_ki_veb>= 24 AND l_ki_veb< 29 THEN 10
					WHEN t1.SC_id_new IN(14,15) AND l_ki_veb>= 29 AND l_ki_veb< 39 THEN 13
					WHEN t1.SC_id_new IN(14,15) AND l_ki_veb>= 39 THEN 18
					WHEN t1.SC_id_new IN(14,15) THEN 18
					WHEN t1.SC_id_new=16 and l_ki_veb = . THEN 18 
					WHEN t1.SC_id_new=16 AND l_ki_veb< 20 THEN 7
					WHEN t1.SC_id_new=16 AND l_ki_veb>= 20 AND l_ki_veb< 34 THEN 11
					WHEN t1.SC_id_new=16 AND l_ki_veb>= 34 THEN 18
					WHEN t1.SC_id_new=16 THEN 18
					WHEN t1.SC_id_new IN(17) and l_ki_veb = . THEN 17
					WHEN t1.SC_id_new IN(17) AND l_ki_veb< 20 THEN 5
					WHEN t1.SC_id_new IN(17) AND l_ki_veb>= 20 AND l_ki_veb< 31 THEN 10
					WHEN t1.SC_id_new IN(17) AND l_ki_veb>= 31 THEN 17
					WHEN t1.SC_id_new IN(17) THEN 17
				END) AS w_l_ki_veb_new,
				/* следующий код для перерасчета w_model_quality_veb_bki */
				/* w_ANTIQUITY_veb_bki (давность кредитной истории) */
				(CASE
					/* correct else */
					WHEN t1.antiquity_veb_bki = . THEN 35
					WHEN t1.antiquity_veb_bki < 1 THEN 32
					WHEN t1.antiquity_veb_bki >= 1 AND t1.antiquity_veb_bki < 2 THEN 35
					WHEN t1.antiquity_veb_bki >= 2 AND t1.antiquity_veb_bki < 4 THEN 44
					WHEN t1.antiquity_veb_bki >= 4 AND t1.antiquity_veb_bki < 18 THEN 58
					WHEN t1.antiquity_veb_bki >= 18 THEN 47
					else 35 end) as w_ANTIQUITY_veb_bki,
				/* вычисляем значение параметра w_DEFOLT_veb_bki (наличие дефолта) */
				(CASE 
					/* correct else */ 
					WHEN t1.DEFOLT_veb_bki = . THEN 37
					WHEN t1.DEFOLT_veb_bki = 0 THEN 38
					WHEN t1.DEFOLT_veb_bki = 1 THEN 23
					else 37 end) as w_DEFOLT_veb_bki,
				/* вычисляем значение веса параметра pScore_n_sr_simv_bank (среднее значение балла за N) */
				(CASE
					/* correct else */ 
					WHEN t1.pScore_n_sr_simv_bank_bki = . THEN . /* должно повторять значение иначе*/
					WHEN t1.pScore_n_sr_simv_bank_bki< 10.29 THEN 40
					WHEN t1.pScore_n_sr_simv_bank_bki>= 10.29 AND t1.pScore_n_sr_simv_bank_bki< 11 THEN 55
					WHEN t1.pScore_n_sr_simv_bank_bki>= 11 AND t1.pScore_n_sr_simv_bank_bki< 12.23 THEN 39
					WHEN t1.pScore_n_sr_simv_bank_bki>= 12.23 AND t1.pScore_n_sr_simv_bank_bki< 19 THEN 27
					WHEN t1.pScore_n_sr_simv_bank_bki>= 19 THEN 15
					else 40 end) as w_pScore_n_sr_simv_bank_bki,
				/* вычисляем значение веса параметра w_DURATION_veb_bki (продолжительность кредитной истории) */
				(CASE
					/* correct else */ 
					WHEN t1.l_ki_veb_bki = . THEN 37
					WHEN t1.l_ki_veb_bki < 2 THEN 5
					WHEN t1.l_ki_veb_bki >= 2 AND t1.l_ki_veb_bki < 5 THEN 12
					WHEN t1.l_ki_veb_bki >= 5 AND t1.l_ki_veb_bki < 10 THEN 26
					WHEN t1.l_ki_veb_bki >= 10 AND t1.l_ki_veb_bki < 18 THEN 37
					WHEN t1.l_ki_veb_bki >= 18 AND t1.l_ki_veb_bki < 26 THEN 45
					WHEN t1.l_ki_veb_bki >= 26 THEN 59
					else 37 end) as w_DURATION_veb_bki,
				(CASE WHEN t1.sc_ID_new in(12,13) AND ((t1.cnt_credit_veb=. OR t1.cnt_credit_veb=1) AND (t1.OD_SUM_VEB=. OR t1.OD_SUM_VEB>=1)) THEN 2
				WHEN t1.sc_ID_new in(12,13) AND ((t1.cnt_credit_veb=. OR t1.cnt_credit_veb=1) AND t1.OD_SUM_VEB<1) THEN 3
				WHEN t1.sc_ID_new in(12,13) AND (t1.cnt_credit_veb<1 AND (t1.OD_SUM_VEB=. OR t1.OD_SUM_VEB>=1)) THEN 2
				WHEN t1.sc_ID_new in(12,13) AND (t1.cnt_credit_veb>=2 AND (t1.OD_SUM_VEB=. OR t1.OD_SUM_VEB>=1)) THEN 1
				WHEN t1.sc_ID_new in(12,13) AND (t1.cnt_credit_veb<1 AND t1.OD_SUM_VEB<1) THEN 1
				WHEN t1.sc_ID_new in(12,13) AND (t1.cnt_credit_veb>=2 AND t1.OD_SUM_VEB<1) THEN 2
				WHEN t1.sc_ID_new in(12,13) THEN 2
				WHEN t1.sc_ID_new in(14,15,16,17) AND ((t1.cnt_credit_veb=. OR t1.cnt_credit_veb=1) AND (t1.OD_SUM_VEB=. OR t1.OD_SUM_VEB>=1)) THEN 2
				WHEN t1.sc_ID_new in(14,15,16,17) AND ((t1.cnt_credit_veb=. OR t1.cnt_credit_veb=1) AND (t1.OD_SUM_VEB<1)) THEN 3
				WHEN t1.sc_ID_new in(14,15,16,17) AND (t1.cnt_credit_veb<1 AND (t1.OD_SUM_VEB=. OR t1.OD_SUM_VEB>=1)) THEN 2
				WHEN t1.sc_ID_new in(14,15,16,17) AND (t1.cnt_credit_veb=2 AND (t1.OD_SUM_VEB=. OR t1.OD_SUM_VEB>=1)) THEN 1
				WHEN t1.sc_ID_new in(14,15,16,17) AND (t1.cnt_credit_veb>=3 AND (t1.OD_SUM_VEB=. OR t1.OD_SUM_VEB>=1)) THEN 1
				WHEN t1.sc_ID_new in(14,15,16,17) AND (t1.cnt_credit_veb<1 AND (t1.OD_SUM_VEB<1)) THEN 1
				WHEN t1.sc_ID_new in(14,15,16,17) AND (t1.cnt_credit_veb=2 AND (t1.OD_SUM_VEB<1)) THEN 2
				WHEN t1.sc_ID_new in(14,15,16,17) AND (t1.cnt_credit_veb>=3 AND (t1.OD_SUM_VEB<1)) THEN 2
				WHEN t1.sc_ID_new in(14,15,16,17) THEN 1
				END) as k_od,
				(CASE
					WHEN t1.sc_ID_new in(12,13) and ((t1.reg_groupe_new=. OR t1.reg_groupe_new=3) AND (t1.CHANNEL=. OR t1.CHANNEL=1)) THEN 2
					WHEN t1.sc_ID_new in(12,13) and ((t1.reg_groupe_new=1) AND (t1.CHANNEL=. OR t1.CHANNEL=1)) THEN 3
					WHEN t1.sc_ID_new in(12,13) and ((t1.reg_groupe_new=2) AND (t1.CHANNEL=. OR t1.CHANNEL=1)) THEN 3
					WHEN t1.sc_ID_new in(12,13) and ((t1.reg_groupe_new=. OR t1.reg_groupe_new=3) AND (t1.CHANNEL=2)) THEN 1
					WHEN t1.sc_ID_new in(12,13) and ((t1.reg_groupe_new=1) AND (t1.CHANNEL=2)) THEN 2
					WHEN t1.sc_ID_new in(12,13) and ((t1.reg_groupe_new=2) AND (t1.CHANNEL=2)) THEN 1
					WHEN t1.sc_ID_new in(12,13) THEN 3
					WHEN t1.sc_ID_new in(14,15) and ((t1.reg_groupe_new=. OR t1.reg_groupe_new=1) AND (t1.CHANNEL=. OR t1.CHANNEL=1)) THEN 3
					WHEN t1.sc_ID_new in(14,15) and ((t1.reg_groupe_new IN(2,3)) AND (t1.CHANNEL=. OR t1.CHANNEL=1)) THEN 2
					WHEN t1.sc_ID_new in(14,15) and ((t1.reg_groupe_new=. OR t1.reg_groupe_new=1) AND (t1.CHANNEL=2)) THEN 2
					WHEN t1.sc_ID_new in(14,15) and ((t1.reg_groupe_new IN(2,3)) AND (t1.CHANNEL=2)) THEN 1
					WHEN t1.sc_ID_new in(14,15) THEN 3
					WHEN t1.sc_ID_new in(16,17) and ((t1.reg_groupe_new=. OR t1.reg_groupe_new=1) AND (t1.CHANNEL=. OR t1.CHANNEL<2)) THEN 2
					WHEN t1.sc_ID_new in(16,17) and ((t1.reg_groupe_new IN(2,3)) AND (t1.CHANNEL=. OR t1.CHANNEL<2)) THEN 1
					WHEN t1.sc_ID_new in(16,17) and ((t1.reg_groupe_new=. OR t1.reg_groupe_new=1) AND (t1.CHANNEL=2)) THEN 1
					WHEN t1.sc_ID_new in(16,17) and ((t1.reg_groupe_new IN(2,3)) AND (t1.CHANNEL=2)) THEN 1
					WHEN t1.sc_ID_new in(16,17) THEN 2
					END) as reg_grp_channel,
				(CASE
					WHEN t1.sc_ID_new in(12,13) and ((t1.summ_aggregate=. OR t1.summ_aggregate<221875) AND t1.srok<36) THEN 5 /*1*/
					WHEN t1.sc_ID_new in(12,13) and ((t1.summ_aggregate>=221875 AND t1.summ_aggregate<388813) AND t1.srok<36) THEN 4 /*2*/
					WHEN t1.sc_ID_new in(12,13) and ((t1.summ_aggregate>=388813) AND t1.srok<36) THEN 3 /*3*/
					WHEN t1.sc_ID_new in(12,13) and ((t1.summ_aggregate=. OR t1.summ_aggregate<221875) AND (t1.srok>=36 AND t1.srok<60)) THEN 3 /*4*/
					WHEN t1.sc_ID_new in(12,13) and ((t1.summ_aggregate>=221875 AND t1.summ_aggregate<388813) AND (t1.srok=. OR t1.srok>=36)) THEN 2 /*5*/
					WHEN t1.sc_ID_new in(12,13) and ((t1.summ_aggregate>=388813) AND (t1.srok=. OR t1.srok>=36)) THEN 1 /*6*/
					WHEN t1.sc_ID_new in(12,13) and ((t1.summ_aggregate<140729) AND (t1.srok=. OR t1.srok>=60)) THEN 4 /*7*/
					WHEN t1.sc_ID_new in(12,13) and ((t1.summ_aggregate=. OR (t1.summ_aggregate>=140729 AND t1.summ_aggregate< 221875)) AND (t1.srok=. OR t1.srok>=60)) THEN 3 /*8*/
					WHEN t1.sc_ID_new in(12,13) THEN 3
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate< 122936) AND t1.srok<36) THEN 5 /*1*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate=. OR (t1.summ_aggregate>=122936 AND t1.summ_aggregate<215030)) AND t1.srok<36) THEN 5 /*2*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=215030 AND t1.summ_aggregate<306601) AND t1.srok<36) THEN 4 /*3*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=306601 AND t1.summ_aggregate<517782) AND t1.srok<36) THEN 3 /*4*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=517782) AND t1.srok<36) THEN 3 /*5*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate< 122936) AND (t1.srok>=36 and t1.srok<60)) THEN 4 /*6*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate=. OR (t1.summ_aggregate>=122936 AND t1.summ_aggregate<215030)) AND (t1.srok>=36 and t1.srok<60)) THEN 3 /*7*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=215030 AND t1.summ_aggregate<306601) AND (t1.srok>=36 and t1.srok<60)) THEN 3 /*8*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=306601 AND t1.summ_aggregate<517782) AND (t1.srok>=36 and t1.srok<60)) THEN 2 /*9*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=517782) AND (t1.srok>=36 and t1.srok<60)) THEN 1 /*10*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate< 122936) AND (t1.srok>=60)) THEN 4 /*11*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate=. OR (t1.summ_aggregate>=122936 AND t1.summ_aggregate<215030)) AND (t1.srok>=60)) THEN 3 /*12*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=215030 AND t1.summ_aggregate<306601) AND (t1.srok>=60)) THEN 3 /*13*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=306601 AND t1.summ_aggregate<517782) AND (t1.srok>=60)) THEN 2 /*14*/
					WHEN t1.sc_ID_new in(14,15) AND ((t1.summ_aggregate>=517782) AND (t1.srok>=60)) THEN 1 /*15*/
					WHEN t1.sc_ID_new in(14,15) THEN 3
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate< 163999) AND t1.srok<36) THEN 5 /*1*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate>=163999 AND t1.summ_aggregate< 212691) AND t1.srok<36) THEN 4 /*2*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate=. OR (t1.summ_aggregate>=212691 AND t1.summ_aggregate<342312)) AND srok<36) THEN 4 /*3*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate>=342312 AND t1.summ_aggregate< 523533) AND t1.srok<36) THEN 3 /*4*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate>=523533) AND t1.srok<36) THEN 1 /*5*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate< 163999) AND (t1.srok=. OR (srok>=36 AND t1.srok<48))) THEN 4 /*6*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate>=163999 AND t1.summ_aggregate< 212691) AND (t1.srok=. OR (t1.srok>=36 AND t1.srok<48))) THEN 4 /*7*/
					WHEN t1.sc_ID_new in(16,17) and ((summ_aggregate=. OR (t1.summ_aggregate>=212691 AND t1.summ_aggregate<342312)) AND (t1.srok=. OR (t1.srok>=36 AND t1.srok<48))) THEN 3 /*8*/
					WHEN t1.sc_ID_new in(16,17) and ((summ_aggregate>=342312 AND t1.summ_aggregate< 523533) AND (t1.srok=. OR (t1.srok>=36 AND srok<48))) THEN 2 /*9*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate>=523533) AND (t1.srok=. OR (t1.srok>=36 AND t1.srok<48))) THEN 1 /*10*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate< 163999) AND t1.srok>=48) THEN 4 /*11*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate>=163999 AND t1.summ_aggregate< 212691) AND t1.srok>=48) THEN 3 /*12*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate=. OR (summ_aggregate>=212691 AND t1.summ_aggregate<342312)) AND t1.srok>=48) THEN 3 /*13*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate>=342312 AND t1.summ_aggregate< 523533) AND t1.srok>=48) THEN 2 /*14*/
					WHEN t1.sc_ID_new in(16,17) and ((t1.summ_aggregate>=523533) AND t1.srok>=48) THEN 1 /*15*/	
					WHEN t1.sc_ID_new in(16,17) THEN 4
				END) as srok_sovocup
				
				/*ДАЛЕЕ РАСЧЕТЫ И ВЕСА ДЛЯ КАРТ 20-28 от 08.2014*/
				,(CASE WHEN t1.SC_id_20_28=25 AND t1.antiquity_veb = . THEN 16
					WHEN t1.SC_id_20_28=25 AND t1.antiquity_veb < 1 THEN 13
					WHEN t1.SC_id_20_28=25 AND t1.antiquity_veb >= 4 AND t1.antiquity_veb < 18 THEN 19
					WHEN t1.SC_id_20_28=25 AND t1.antiquity_veb >= 18 AND t1.antiquity_veb < 33 THEN 11
					WHEN t1.SC_id_20_28=25 AND t1.antiquity_veb >= 33 THEN -4
					WHEN t1.SC_id_20_28=25 THEN 16 END) as w2_antiquity_veb /*ОК+ 25*/
				,(CASE WHEN t1.SC_id_20_28=22 AND t1.antiquity_veb_bki = . THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.antiquity_veb_bki < 1 THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.antiquity_veb_bki >= 1 AND t1.antiquity_veb_bki < 2 THEN 11
					WHEN t1.SC_id_20_28=22 AND t1.antiquity_veb_bki >= 2 THEN 9
					WHEN t1.SC_id_20_28=22 THEN 12 END) as w2_antiquity_veb_bki /*ОК+ 22*/
				,(CASE WHEN t1.SC_id_20_28=28 AND t1.INQUIRER_POINT = . THEN 10
					WHEN t1.SC_id_20_28=28 AND t1.INQUIRER_POINT < 0.9 THEN 15
					WHEN t1.SC_id_20_28=28 THEN 10 END) as w2_INQUIRER_POINT /*ОК 28*/
				,(CASE WHEN t1.SC_id_20_28=23 AND t1.aver_6_rate = . THEN 13
					WHEN t1.SC_id_20_28=23 AND t1.aver_6_rate < 10 THEN 19
					WHEN t1.SC_id_20_28=23 AND t1.aver_6_rate >= 20 THEN 6
					WHEN t1.SC_id_20_28=23 THEN 13 END) as w2_aver_6_rate /*ОК 23*/
				,(CASE WHEN t1.SC_id_20_28=24 AND t1.cnt_credit_veb = . THEN 19
					WHEN t1.SC_id_20_28=24 AND t1.cnt_credit_veb >= 2 AND t1.cnt_credit_veb < 3 THEN 5
					WHEN t1.SC_id_20_28=24 AND t1.cnt_credit_veb >= 3 THEN -11
					WHEN t1.SC_id_20_28=24 THEN 19 END) as w2_cnt_credit_veb /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=27 AND t1.defolt_veb = . THEN 18
					WHEN t1.SC_id_20_28=27 AND t1.defolt_veb >= 1 THEN -3
					WHEN t1.SC_id_20_28=27 THEN 18 END) as w2_defolt_veb /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.SALE_ID_FIRST = . THEN 16
					WHEN t1.SC_id_20_28=20 AND t1.SALE_ID_FIRST IN(3,13) THEN 7
					WHEN t1.SC_id_20_28=20 AND t1.SALE_ID_FIRST = 10 THEN 0
					WHEN t1.SC_id_20_28=20 THEN 16
					WHEN t1.SC_id_20_28=21 AND t1.SALE_ID_FIRST = . THEN 16
					WHEN t1.SC_id_20_28=21 AND t1.SALE_ID_FIRST IN(10,17) THEN 0
					WHEN t1.SC_id_20_28=21 AND t1.SALE_ID_FIRST IN(3,16) THEN 12
					WHEN t1.SC_id_20_28=21 THEN 16
					WHEN t1.SC_id_20_28=22 AND t1.SALE_ID_FIRST = . THEN 17
					WHEN t1.SC_id_20_28=22 AND t1.SALE_ID_FIRST = 10 THEN -1
					WHEN t1.SC_id_20_28=22 AND t1.SALE_ID_FIRST IN(3,17) THEN 10
					WHEN t1.SC_id_20_28=22 THEN 17
					WHEN t1.SC_id_20_28=24 AND t1.SALE_ID_FIRST = . THEN 14
					WHEN t1.SC_id_20_28=24 AND t1.SALE_ID_FIRST IN(10,16,17) THEN 5
					WHEN t1.SC_id_20_28=24 AND t1.SALE_ID_FIRST IN(3) THEN 9
					WHEN t1.SC_id_20_28=24 AND t1.SALE_ID_FIRST IN(1,11,13,2) THEN 14
					WHEN t1.SC_id_20_28=24 THEN 14
					WHEN t1.SC_id_20_28=26 AND t1.SALE_ID_FIRST = . THEN 6
					WHEN t1.SC_id_20_28=26 AND t1.SALE_ID_FIRST IN(11,16,17,3) THEN 6
					WHEN t1.SC_id_20_28=26 AND t1.SALE_ID_FIRST IN(10) THEN 8
					WHEN t1.SC_id_20_28=26 AND t1.SALE_ID_FIRST IN(1,13,2,26) THEN 14
					WHEN t1.SC_id_20_28=26 THEN 6
					END) as w2_format_groups /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.org_vid = . THEN 3
					WHEN t1.SC_id_20_28=20 AND t1.org_vid IN(47, 50, 54, 66, 70, 81) THEN 8
					WHEN t1.SC_id_20_28=20 AND t1.org_vid IN(0, 19, 21, 23, 26, 40, 42, 46, 57, 63, 64, 71, 72, 73, 74, 75, 76, 77, 79, 82, 85, 86, 91, 103, 105) THEN 13
					WHEN t1.SC_id_20_28=20 AND t1.org_vid IN(17, 18, 20, 39, 49, 56, 60, 78, 83, 88, 89, 90, 92, 93, 94, 96, 97,104) THEN 25
					WHEN t1.SC_id_20_28=20 AND t1.org_vid IN(22, 24, 25, 27, 38, 53, 58, 59, 61, 65, 80, 84, 95, 101, 102) THEN 6
					WHEN t1.SC_id_20_28=20 THEN 3
					WHEN t1.SC_id_20_28=21 AND t1.org_vid = . THEN 5
					WHEN t1.SC_id_20_28=21 AND t1.org_vid IN(22, 23, 24, 25, 30, 32, 33, 50, 54, 56, 62, 66, 67, 69, 70, 79, 80, 85, 95, 97, 98, 99, 105) THEN 9
					WHEN t1.SC_id_20_28=21 AND t1.org_vid IN(21, 26, 27, 37, 38, 42, 45, 46, 49, 58, 59, 65, 71, 73, 84, 102) THEN 12
					WHEN t1.SC_id_20_28=21 AND t1.org_vid IN(107, 17, 18, 19, 39, 40, 47, 61, 63, 64, 72, 74, 75, 76, 78, 82, 83, 87, 90, 91, 92, 93, 94, 96, 100, 101) THEN 14
					WHEN t1.SC_id_20_28=21 THEN 5
					WHEN t1.SC_id_20_28=22 AND t1.org_vid = . THEN 2
					WHEN t1.SC_id_20_28=22 AND t1.org_vid IN(20, 28, 31, 33, 43, 47, 49, 51, 55, 56, 69, 99) THEN 6
					WHEN t1.SC_id_20_28=22 AND t1.org_vid IN(22, 25, 32, 35, 42, 45, 50, 54, 62, 65, 70, 81, 84, 95, 105) THEN 9
					WHEN t1.SC_id_20_28=22 AND t1.org_vid IN(17, 18, 19, 37, 39, 40, 52, 59, 72, 74, 77, 78, 80, 82, 83, 85, 86, 91, 92, 93, 94, 96, 97) THEN 20
					WHEN t1.SC_id_20_28=22 AND t1.org_vid IN(21, 23, 24, 26, 38, 46, 57, 58, 60, 61, 63, 64, 66, 73, 75, 87, 88, 89, 102) THEN 13
					WHEN t1.SC_id_20_28=22 AND t1.org_vid IN(27, 71, 76, 79, 90, 100, 101) THEN 17
					WHEN t1.SC_id_20_28=22 THEN 2
					WHEN t1.SC_id_20_28=24 AND t1.org_vid = . THEN 5
					WHEN t1.SC_id_20_28=24 AND t1.org_vid IN(17, 18, 19, 39, 44, 47, 49, 52, 59, 61, 71, 72, 73, 74, 75, 78, 79, 80, 82, 83, 87, 89, 90, 92, 93, 94, 100, 102) THEN 17
					WHEN t1.SC_id_20_28=24 AND t1.org_vid IN(0, 21, 22, 23, 24, 26, 28, 30, 43, 46, 50, 53, 54, 57, 58, 62, 63, 64, 65, 66, 70, 76, 77, 85, 91, 98, 101) THEN 9
					WHEN t1.SC_id_20_28=24 THEN 5
					WHEN t1.SC_id_20_28=25 AND t1.org_vid = . THEN 32
					WHEN t1.SC_id_20_28=25 AND t1.org_vid IN(27, 30, 32, 33, 34, 37, 38, 41, 42, 43, 48, 55, 58, 65, 67, 68, 79, 81, 87, 95, 97, 98, 101, 105) THEN 3
					WHEN t1.SC_id_20_28=25 AND t1.org_vid IN(22, 23, 28, 31, 36, 51, 54, 74, 102, 103) THEN 10
					WHEN t1.SC_id_20_28=25 AND t1.org_vid IN(19, 39, 45, 62, 71, 72, 75, 76, 80, 90, 94) THEN 21
					WHEN t1.SC_id_20_28=25 AND t1.org_vid IN(21, 25, 29, 46, 50, 53, 57, 59, 64, 66, 70, 99) THEN 13
					WHEN t1.SC_id_20_28=25 THEN 32
					WHEN t1.SC_id_20_28=26 AND t1.org_vid = . THEN 5
					WHEN t1.SC_id_20_28=26 AND t1.org_vid IN(22, 23, 38, 44, 45, 50, 53, 54, 58, 62, 66, 68, 69, 70, 73, 76, 77, 82) THEN 11
					WHEN t1.SC_id_20_28=26 AND t1.org_vid IN(19, 20, 21, 24, 26, 40, 46, 57, 59, 61, 63, 64, 65, 71, 72, 75, 80, 85, 91, 97, 102, 105,) THEN 17
					WHEN t1.SC_id_20_28=26 AND t1.org_vid IN(0, 17, 18, 35, 37, 39, 47, 74, 78, 83, 86, 87, 88, 90, 92, 93, 94, 96, 100, 103) THEN 25
					WHEN t1.SC_id_20_28=26 THEN 5
					WHEN t1.SC_id_20_28=27 AND t1.org_vid = . THEN 16
					WHEN t1.SC_id_20_28=27 AND t1.org_vid IN(0, 18, 22, 25, 29, 30, 33, 34, 35, 36, 38, 40, 41, 43, 49, 50, 51, 55, 57, 68, 80, 91, 93, 95, 99, 105) THEN -3
					WHEN t1.SC_id_20_28=27 AND t1.org_vid IN(17, 19, 20, 23, 24, 28, 39, 42, 45, 46, 53, 58, 59, 61, 63, 65, 69, 71, 72, 73, 75, 76, 78, 81, 85, 87, 90, 92, 94, 98) THEN 33
					WHEN t1.SC_id_20_28=27 THEN 16
					WHEN t1.SC_id_20_28=28 AND t1.org_vid = . THEN 17
					WHEN t1.SC_id_20_28=28 AND t1.org_vid IN(0, 29, 30, 34, 35, 36, 41, 43, 48, 51, 55, 65, 69, 77, 80, 81, 85, 95, 96, 99) THEN 4
					WHEN t1.SC_id_20_28=28 AND t1.org_vid IN(20, 22, 31, 32, 38, 40, 45, 53, 54, 61, 62, 64, 68, 70, 75, 88, 91, 101) THEN 12
					WHEN t1.SC_id_20_28=28 THEN 17 END) as w2_org_vid /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=23 AND t1.reliability_bki = . THEN 11
					WHEN t1.SC_id_20_28=23 AND t1.reliability_bki < 83.33 THEN 17
					WHEN t1.SC_id_20_28=23 AND t1.reliability_bki > 83.33 AND t1.reliability_bki < 100 THEN 13
					WHEN t1.SC_id_20_28=23 THEN 11 END) as w2_reliability_bki /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.bnk_type = . THEN 10
					WHEN t1.SC_id_20_28=20 AND t1.bnk_type IN(1,2) THEN -3
					WHEN t1.SC_id_20_28=20 THEN 10 END) as w2_bnk_type /*ОК*/
				,(CASE 
					WHEN t1.SC_id_20_28=20 AND (charge3_12 = .) THEN 4
					WHEN t1.SC_id_20_28=20 AND charge3_12 < 0.8 THEN 13
					WHEN t1.SC_id_20_28=20 AND charge3_12 >= 0.8 AND charge3_12 < 1.01 THEN 10
					WHEN t1.SC_id_20_28=20 AND charge3_12 >= 1.01 THEN 5 /*в ТЗ ошибка! 1.05*/
					WHEN t1.SC_id_20_28=20 THEN 4
					WHEN t1.SC_id_20_28=21 AND (charge3_12 = .) THEN 4
					WHEN t1.SC_id_20_28=21 AND charge3_12 < 0.62 THEN 14
					WHEN t1.SC_id_20_28=21 AND charge3_12 >= 0.62 AND charge3_12 < 1.02 THEN 12
					WHEN t1.SC_id_20_28=21 AND charge3_12 >= 1.02 THEN 7
					WHEN t1.SC_id_20_28=21 THEN 4
					WHEN t1.SC_id_20_28=22 AND (charge3_12 = .) THEN 6
					WHEN t1.SC_id_20_28=22 AND t1.charge3_12 < 0.64 THEN 16
					WHEN t1.SC_id_20_28=22 AND t1.charge3_12 >= 0.64 AND t1.charge3_12 < 1.02 THEN 13
					WHEN t1.SC_id_20_28=22 AND t1.charge3_12 >= 1.02 AND t1.charge3_12 < 1.26 THEN 6
					WHEN t1.SC_id_20_28=22 AND t1.charge3_12 >= 1.26 THEN 4
					WHEN t1.SC_id_20_28=22 THEN 6 END) as w2_charge3_12 /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.d = . THEN 10
					WHEN t1.SC_id_20_28=20 AND t1.d >= 0.16 AND t1.d < 0.21 THEN 9
					WHEN t1.SC_id_20_28=20 AND t1.d >= 0.21 AND t1.d < 0.26 THEN 8
					WHEN t1.SC_id_20_28=20 AND t1.d >= 0.26 THEN 7
					WHEN t1.SC_id_20_28=20 THEN 10
					WHEN t1.SC_id_20_28=21 AND t1.d = . THEN 9
					WHEN t1.SC_id_20_28=21 AND t1.d < 0.09 THEN 11
					WHEN t1.SC_id_20_28=21 AND t1.d >= 0.09 AND t1.d < 0.15 THEN 10
					WHEN t1.SC_id_20_28=21 AND t1.d >= 0.17 THEN 8
					WHEN t1.SC_id_20_28=21 THEN 9
					WHEN t1.SC_id_20_28=22 AND t1.d = . THEN 11
					WHEN t1.SC_id_20_28=22 AND t1.d < 0.1 THEN 13
					WHEN t1.SC_id_20_28=22 AND t1.d >= 0.14 AND t1.d < 0.19 THEN 10
					WHEN t1.SC_id_20_28=22 AND t1.d >= 0.19 THEN 8
					WHEN t1.SC_id_20_28=22 THEN 11
					WHEN t1.SC_id_20_28=23 AND t1.d = . THEN 14
					WHEN t1.SC_id_20_28=23 AND t1.d < 0.11 THEN 20
					WHEN t1.SC_id_20_28=23 AND t1.d >= 0.18 THEN 5
					WHEN t1.SC_id_20_28=23 THEN 14 END) as w2_d /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=23 AND t1.dynamics_charge_bki = . THEN 12
					WHEN t1.SC_id_20_28=23 AND t1.dynamics_charge_bki < 1 THEN 16
					WHEN t1.SC_id_20_28=23 THEN 12
					WHEN t1.SC_id_20_28=24 AND t1.dynamics_charge_bki = . THEN 10
					WHEN t1.SC_id_20_28=24 AND t1.dynamics_charge_bki < 1 THEN 14
					WHEN t1.SC_id_20_28=24 AND t1.dynamics_charge_bki >= 1.07 THEN 9
					WHEN t1.SC_id_20_28=24 THEN 10
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_charge_bki = . THEN 14
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_charge_bki < 1 THEN 18
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_charge_bki >= 1.15 AND t1.dynamics_charge_bki < 1.29 THEN 11
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_charge_bki >= 1.29 AND t1.dynamics_charge_bki < 1.5 THEN 8
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_charge_bki >= 1.5 THEN 4
					WHEN t1.SC_id_20_28=26 THEN 14
					WHEN t1.SC_id_20_28=27 AND t1.dynamics_charge_bki = . THEN 14
					WHEN t1.SC_id_20_28=27 AND t1.dynamics_charge_bki < 0.87 THEN 19
					WHEN t1.SC_id_20_28=27 AND t1.dynamics_charge_bki >= 1.09 THEN 12
					WHEN t1.SC_id_20_28=27 THEN 14
					WHEN t1.SC_id_20_28=28 AND t1.dynamics_charge_bki = . THEN 17
					WHEN t1.SC_id_20_28=28 AND t1.dynamics_charge_bki < 1 THEN 20
					WHEN t1.SC_id_20_28=28 AND t1.dynamics_charge_bki > 1.1 AND t1.dynamics_charge_bki < 1.22 THEN 14
					WHEN t1.SC_id_20_28=28 AND t1.dynamics_charge_bki > 1.22 AND t1.dynamics_charge_bki < 1.43 THEN 12
					WHEN t1.SC_id_20_28=28 AND t1.dynamics_charge_bki >= 1.43 THEN 7
					WHEN t1.SC_id_20_28=28 THEN 17 END) as w2_dynamics_charge_bki /*ОК+ 100%*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.dynamics_quality_bki = . THEN 10
					WHEN t1.SC_id_20_28=20 AND t1.dynamics_quality_bki >= 0.87 AND t1.dynamics_quality_bki < 1.06 THEN 8
					WHEN t1.SC_id_20_28=20 AND t1.dynamics_quality_bki >= 1.06 THEN 10
					WHEN t1.SC_id_20_28=20 THEN 10
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_quality_bki = . THEN 7
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_quality_bki < 1 THEN 12
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_quality_bki >= 1 AND t1.dynamics_quality_bki < 1.02 THEN 17
					WHEN t1.SC_id_20_28=26 AND t1.dynamics_quality_bki >= 1.02 AND t1.dynamics_quality_bki < 1.1 THEN 10
					WHEN t1.SC_id_20_28=26 THEN 7 END) as w2_dynamics_quality_bki /*ОК+ 100%*/
				,(CASE WHEN t1.SC_id_20_28=27 AND t1.dynamics_quality_veb = . THEN 21
					WHEN t1.SC_id_20_28=27 AND t1.dynamics_quality_veb < 0.86 THEN 6
					WHEN t1.SC_id_20_28=27 AND t1.dynamics_quality_veb >= 1.08 THEN 4
					WHEN t1.SC_id_20_28=27 THEN 21 END) as w2_dynamics_quality_veb /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.education = . THEN 0
					WHEN t1.SC_id_20_28=20 AND t1.education IN(1,5) THEN 6
					WHEN t1.SC_id_20_28=20 AND t1.education = 3 THEN 12
					WHEN t1.SC_id_20_28=20 AND t1.education = 2 THEN 21
					WHEN t1.SC_id_20_28=20 THEN 0
					WHEN t1.SC_id_20_28=21 AND t1.education = . THEN 6
					WHEN t1.SC_id_20_28=21 AND t1.education = 5 THEN 10
					WHEN t1.SC_id_20_28=21 AND t1.education = 2 THEN 18
					WHEN t1.SC_id_20_28=21 THEN 6
					WHEN t1.SC_id_20_28=24 AND t1.education = . THEN 18
					WHEN t1.SC_id_20_28=24 AND t1.education IN(3,5) THEN 10
					WHEN t1.SC_id_20_28=24 AND t1.education IN(1,4,6) THEN 6
					WHEN t1.SC_id_20_28=24 THEN 18
					WHEN t1.SC_id_20_28=25 AND t1.education = . THEN 26
					WHEN t1.SC_id_20_28=25 AND t1.education IN(4,6) THEN 5
					WHEN t1.SC_id_20_28=25 AND t1.education = 5 THEN 11
					WHEN t1.SC_id_20_28=25 AND t1.education IN(1,3) THEN 12
					WHEN t1.SC_id_20_28=25 THEN 26
					WHEN t1.SC_id_20_28=26 AND t1.education = . THEN 20
					WHEN t1.SC_id_20_28=26 AND t1.education IN(3,5) THEN 11
					WHEN t1.SC_id_20_28=26 AND t1.education IN(1,4,6) THEN 7
					WHEN t1.SC_id_20_28=26 THEN 20
					WHEN t1.SC_id_20_28=27 AND t1.education = . THEN 10
					WHEN t1.SC_id_20_28=27 AND t1.education = 5 THEN 16
					WHEN t1.SC_id_20_28=27 AND t1.education IN(2,3) THEN 22
					WHEN t1.SC_id_20_28=27 THEN 10 END) as w2_education /*ОК+*/
				/*,(CASE WHEN t1.SC_id_20_28=24 AND t1.format_groups = . THEN 14
					WHEN t1.SC_id_20_28=24 AND t1.format_groups IN(10,16,17) THEN 5
					WHEN t1.SC_id_20_28=24 AND t1.format_groups =3 THEN 9
					WHEN t1.SC_id_20_28=24 THEN 14
					WHEN t1.SC_id_20_28=26 AND t1.format_groups = . THEN 6
					WHEN t1.SC_id_20_28=26 AND t1.format_groups = 10 THEN 8
					WHEN t1.SC_id_20_28=26 AND t1.format_groups IN(1,2,13,26) THEN 14
					WHEN t1.SC_id_20_28=26 THEN 6 END) as w2_format_groups*/ /*ОК пофиксили на sale_id_first*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.household = . THEN 2
					WHEN t1.SC_id_20_28=20 AND t1.household IN(4,6,10,12) THEN 6
					WHEN t1.SC_id_20_28=20 AND t1.household IN(2,9,11) THEN 8
					WHEN t1.SC_id_20_28=20 AND t1.household IN(5,7,8) THEN 12
					WHEN t1.SC_id_20_28=20 THEN 2
					WHEN t1.SC_id_20_28=22 AND t1.household = . THEN 3
					WHEN t1.SC_id_20_28=22 AND t1.household IN(5,10,11) THEN 7
					WHEN t1.SC_id_20_28=22 AND t1.household IN(7,8,12) THEN 14
					WHEN t1.SC_id_20_28=22 AND t1.household IN(4,6,9) THEN 17
					WHEN t1.SC_id_20_28=22 THEN 3
					WHEN t1.SC_id_20_28=25 AND t1.household = . THEN 16
					WHEN t1.SC_id_20_28=25 AND t1.household IN(2,3) THEN 7
					WHEN t1.SC_id_20_28=25 AND t1.household IN(1,5,8) THEN 9
					WHEN t1.SC_id_20_28=25 AND t1.household IN(4,6,10) THEN 13
					WHEN t1.SC_id_20_28=25 THEN 16
					WHEN t1.SC_id_20_28=27 AND t1.household = . THEN 4
					WHEN t1.SC_id_20_28=27 AND t1.household IN(1,2,11) THEN 10
					WHEN t1.SC_id_20_28=27 AND t1.household IN(5,7,9) THEN 20
					WHEN t1.SC_id_20_28=27 AND t1.household IN(3,4,6) THEN 23
					WHEN t1.SC_id_20_28=27 THEN 4 END) as w2_household /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.max_12_rate = . THEN 6 /*для СПР 6, для xls 12*/
					WHEN t1.SC_id_20_28=20 AND t1.max_12_rate < 10 THEN 6
					WHEN t1.SC_id_20_28=20 AND t1.max_12_rate >= 30 THEN 5
					WHEN t1.SC_id_20_28=20 THEN 12
					WHEN t1.SC_id_20_28=21 AND t1.max_12_rate = . THEN 9 /*для СПР 9, для xls 12*/
					WHEN t1.SC_id_20_28=21 AND t1.max_12_rate < 10 THEN 9
					WHEN t1.SC_id_20_28=21 AND t1.max_12_rate >= 30 THEN 8
					WHEN t1.SC_id_20_28=21 THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.max_12_rate = . THEN 11 /*для СПР 11 = для xls 11*/
					WHEN t1.SC_id_20_28=22 AND t1.max_12_rate >= 10 AND t1.max_12_rate < 30 THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.max_12_rate >= 30 THEN 9
					WHEN t1.SC_id_20_28=22 THEN 11
					WHEN t1.SC_id_20_28=25 AND t1.max_12_rate = . THEN 15 /*для СПР 15 = для xls 15*/
					WHEN t1.SC_id_20_28=25 AND t1.max_12_rate < 10 THEN 12
					WHEN t1.SC_id_20_28=25 AND t1.max_12_rate >= 20 AND t1.max_12_rate < 30 THEN 14
					WHEN t1.SC_id_20_28=25 AND t1.max_12_rate >= 30 AND t1.max_12_rate < 60 THEN 12
					WHEN t1.SC_id_20_28=25 AND t1.max_12_rate >= 60 THEN 8
					WHEN t1.SC_id_20_28=25 THEN 15 END) as w2_max_12_rate /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=21 AND (t1.monthswentwhenlastdeliq = . OR t1.monthswentwhenlastdeliq = 0) THEN 15 /*для СПР 12, для xls 15*/
					WHEN t1.SC_id_20_28=21 AND t1.monthswentwhenlastdeliq < 1 THEN 12
					WHEN t1.SC_id_20_28=21 AND t1.monthswentwhenlastdeliq >= 1 AND t1.monthswentwhenlastdeliq < 8 THEN 0
					WHEN t1.SC_id_20_28=21 AND t1.monthswentwhenlastdeliq >= 8 THEN 7
					WHEN t1.SC_id_20_28=21 THEN 15
					WHEN t1.SC_id_20_28=22 AND (t1.monthswentwhenlastdeliq = . OR t1.monthswentwhenlastdeliq = 0) THEN 16 /*для СПР 12, для xls 16*/
					WHEN t1.SC_id_20_28=22 AND t1.monthswentwhenlastdeliq < 1 THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.monthswentwhenlastdeliq >= 1 AND t1.monthswentwhenlastdeliq < 7 THEN 0
					WHEN t1.SC_id_20_28=22 AND t1.monthswentwhenlastdeliq >= 7 THEN 6
					WHEN t1.SC_id_20_28=22 THEN 16
					WHEN t1.SC_id_20_28=23 AND (t1.monthswentwhenlastdeliq = . OR t1.monthswentwhenlastdeliq = 0) THEN 16 /*для СПР 15, для xls 16*/
					WHEN t1.SC_id_20_28=23 AND t1.monthswentwhenlastdeliq < 1 THEN 15
					WHEN t1.SC_id_20_28=23 AND t1.monthswentwhenlastdeliq >= 1 AND t1.monthswentwhenlastdeliq < 6 THEN 9
					WHEN t1.SC_id_20_28=23 AND t1.monthswentwhenlastdeliq >= 6 THEN 12
					WHEN t1.SC_id_20_28=23 THEN 16
					WHEN t1.SC_id_20_28=27 AND (t1.monthswentwhenlastdeliq = . OR t1.monthswentwhenlastdeliq = 0) THEN 20 /*для СПР 18, для xls 20*/
					WHEN t1.SC_id_20_28=27 AND t1.monthswentwhenlastdeliq < 2 THEN 18
					WHEN t1.SC_id_20_28=27 AND t1.monthswentwhenlastdeliq >= 2 AND t1.monthswentwhenlastdeliq < 8 THEN 9
					WHEN t1.SC_id_20_28=27 AND t1.monthswentwhenlastdeliq >= 8 THEN 15
					WHEN t1.SC_id_20_28=27 THEN 20
					WHEN t1.SC_id_20_28=28 AND (t1.monthswentwhenlastdeliq = . OR t1.monthswentwhenlastdeliq = 0) THEN 22 /*для СПР 21, для xls 22*/
					WHEN t1.SC_id_20_28=28 AND t1.monthswentwhenlastdeliq < 1 THEN 21
					WHEN t1.SC_id_20_28=28 AND t1.monthswentwhenlastdeliq >= 1 AND t1.monthswentwhenlastdeliq < 4 THEN 8
					WHEN t1.SC_id_20_28=28 AND t1.monthswentwhenlastdeliq >= 4 AND t1.monthswentwhenlastdeliq < 16 THEN 13
					WHEN t1.SC_id_20_28=28 AND t1.monthswentwhenlastdeliq >= 16 THEN 17
					WHEN t1.SC_id_20_28=28 THEN 22 END) as w2_monthswentwhenlastdeliq /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=23 AND t1.od_sum_veb = . THEN -5
					WHEN t1.SC_id_20_28=23 AND t1.od_sum_veb <0.63 THEN 21
					WHEN t1.SC_id_20_28=23 AND t1.od_sum_veb >= 0.63 AND t1.od_sum_veb < 0.9 THEN 10
					WHEN t1.SC_id_20_28=23 THEN -5
					WHEN t1.SC_id_20_28=24 AND t1.od_sum_veb = . THEN 12
					WHEN t1.SC_id_20_28=24 AND t1.od_sum_veb <0.47 THEN 27
					WHEN t1.SC_id_20_28=24 AND t1.od_sum_veb >= 0.92 AND t1.od_sum_veb < 0.96 THEN 3
					WHEN t1.SC_id_20_28=24 AND t1.od_sum_veb >= 0.96 THEN -3
					WHEN t1.SC_id_20_28=24 THEN 12
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb = . THEN 20
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb >= 0.22 AND t1.od_sum_veb < 0.38 THEN 16
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb >= 0.38 AND t1.od_sum_veb < 0.49 THEN 12
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb >= 0.49 AND t1.od_sum_veb < 0.69 THEN 8
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb >= 0.69 THEN 3
					WHEN t1.SC_id_20_28=26 THEN 20
					WHEN t1.SC_id_20_28=27 AND t1.od_sum_veb = . THEN 17
					WHEN t1.SC_id_20_28=27 AND t1.od_sum_veb >= 0.03 AND t1.od_sum_veb < 0.18 THEN 25
					WHEN t1.SC_id_20_28=27 AND t1.od_sum_veb >= 0.18 AND t1.od_sum_veb < 0.36 THEN 17
					WHEN t1.SC_id_20_28=27 AND t1.od_sum_veb >= 0.36 THEN -5
					WHEN t1.SC_id_20_28=27 THEN 17
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb = . THEN 26
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb >= 0.24 AND t1.od_sum_veb < 0.38 THEN 20
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb >= 0.38 AND t1.od_sum_veb < 0.49 THEN 14
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb >= 0.49 AND t1.od_sum_veb < 0.75 THEN 8
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb >= 0.75 THEN 2
					WHEN t1.SC_id_20_28=28 THEN 26 END) as w2_od_sum_veb /*ОК*/

				,(CASE WHEN t1.SC_id_20_28=23 AND t1.od_sum_veb_all = . THEN -5
					WHEN t1.SC_id_20_28=23 AND t1.od_sum_veb_all <0.63 THEN 21
					WHEN t1.SC_id_20_28=23 AND t1.od_sum_veb_all >= 0.63 AND t1.od_sum_veb_all < 0.9 THEN 10
					WHEN t1.SC_id_20_28=23 THEN -5
					WHEN t1.SC_id_20_28=24 AND t1.od_sum_veb_all = . THEN 12
					WHEN t1.SC_id_20_28=24 AND t1.od_sum_veb_all <0.47 THEN 27
					WHEN t1.SC_id_20_28=24 AND t1.od_sum_veb_all >= 0.92 AND t1.od_sum_veb_all < 0.96 THEN 3
					WHEN t1.SC_id_20_28=24 AND t1.od_sum_veb_all >= 0.96 THEN -3
					WHEN t1.SC_id_20_28=24 THEN 12
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb_all = . THEN 20
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb_all >= 0.22 AND t1.od_sum_veb_all < 0.38 THEN 16
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb_all >= 0.38 AND t1.od_sum_veb_all < 0.49 THEN 12
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb_all >= 0.49 AND t1.od_sum_veb_all < 0.69 THEN 8
					WHEN t1.SC_id_20_28=26 AND t1.od_sum_veb_all >= 0.69 THEN 3
					WHEN t1.SC_id_20_28=26 THEN 20
					WHEN t1.SC_id_20_28=27 AND t1.od_sum_veb_all = . THEN 17
					WHEN t1.SC_id_20_28=27 AND t1.od_sum_veb_all >= 0.03 AND t1.od_sum_veb_all < 0.18 THEN 25
					WHEN t1.SC_id_20_28=27 AND t1.od_sum_veb_all >= 0.18 AND t1.od_sum_veb_all < 0.36 THEN 17
					WHEN t1.SC_id_20_28=27 AND t1.od_sum_veb_all >= 0.36 THEN -5
					WHEN t1.SC_id_20_28=27 THEN 17
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb_all = . THEN 26
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb_all >= 0.24 AND t1.od_sum_veb_all < 0.38 THEN 20
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb_all >= 0.38 AND t1.od_sum_veb_all < 0.49 THEN 14
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb_all >= 0.49 AND t1.od_sum_veb_all < 0.75 THEN 8
					WHEN t1.SC_id_20_28=28 AND t1.od_sum_veb_all >= 0.75 THEN 2
					WHEN t1.SC_id_20_28=28 THEN 26 END) as w2_od_sum_veb_all /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=21 AND t1.pensioner = . THEN 9
					WHEN t1.SC_id_20_28=21 AND t1.pensioner IN(1,2) THEN 12
					WHEN t1.SC_id_20_28=21 THEN 9 END) as w2_pensioner /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=27 AND t1.Period_decl = . THEN 14
					WHEN t1.SC_id_20_28=27 AND t1.Period_decl <24 THEN 24
					WHEN t1.SC_id_20_28=27 AND t1.Period_decl >= 24 AND t1.Period_decl < 36 THEN 9
					WHEN t1.SC_id_20_28=27 THEN 14
					WHEN t1.SC_id_20_28=28 AND t1.Period_decl = . THEN 18
					WHEN t1.SC_id_20_28=28 AND t1.Period_decl >= 24 AND t1.Period_decl < 36 THEN 15
					WHEN t1.SC_id_20_28=28 AND t1.Period_decl >= 36 THEN 14
					WHEN t1.SC_id_20_28=28 AND t1.Period_decl >= 36 THEN 14
					WHEN t1.SC_id_20_28=28 THEN 18 END) as w2_period /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=23 AND t1.prodoljitelnost_ki_veb = . THEN 13
					WHEN t1.SC_id_20_28=23 AND t1.prodoljitelnost_ki_veb < 8 THEN 8
					WHEN t1.SC_id_20_28=23 AND t1.prodoljitelnost_ki_veb >= 8 AND t1.prodoljitelnost_ki_veb < 10 THEN 10
					WHEN t1.SC_id_20_28=23 AND t1.prodoljitelnost_ki_veb >= 16 THEN 19
					WHEN t1.SC_id_20_28=23 THEN 13
					WHEN t1.SC_id_20_28=24 AND t1.prodoljitelnost_ki_veb = . THEN 7
					WHEN t1.SC_id_20_28=24 AND t1.prodoljitelnost_ki_veb >= 9 AND t1.prodoljitelnost_ki_veb < 11 THEN 9
					WHEN t1.SC_id_20_28=24 AND t1.prodoljitelnost_ki_veb >= 11 AND t1.prodoljitelnost_ki_veb < 13 THEN 11
					WHEN t1.SC_id_20_28=24 AND t1.prodoljitelnost_ki_veb >= 13 THEN 17
					WHEN t1.SC_id_20_28=24 THEN 7 END) as w2_prodoljitelnost_ki_veb /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.qm_new_bki = . THEN 2
					WHEN t1.SC_id_20_28=20 AND t1.qm_new_bki < 122 THEN -5
					WHEN t1.SC_id_20_28=20 AND t1.qm_new_bki >= 122 AND t1.qm_new_bki < 131 THEN 2
					WHEN t1.SC_id_20_28=20 AND t1.qm_new_bki >= 131 AND t1.qm_new_bki < 147 THEN 12
					WHEN t1.SC_id_20_28=20 AND t1.qm_new_bki >= 147 THEN 25
					WHEN t1.SC_id_20_28=20 THEN 2 
					WHEN t1.SC_id_20_28=21 AND t1.qm_new_bki = . THEN 0
					WHEN t1.SC_id_20_28=21 AND t1.qm_new_bki < 131 THEN -7
					WHEN t1.SC_id_20_28=21 AND t1.qm_new_bki >= 131 AND t1.qm_new_bki < 145 THEN 7
					WHEN t1.SC_id_20_28=21 AND t1.qm_new_bki >= 145 AND t1.qm_new_bki < 159 THEN 18
					WHEN t1.SC_id_20_28=21 AND t1.qm_new_bki >= 159 THEN 29
					WHEN t1.SC_id_20_28=21 THEN 0
					WHEN t1.SC_id_20_28=22 AND t1.qm_new_bki = . THEN 5
					WHEN t1.SC_id_20_28=22 AND t1.qm_new_bki < 122 THEN -9
					WHEN t1.SC_id_20_28=22 AND t1.qm_new_bki >= 122 AND t1.qm_new_bki < 134 THEN 1
					WHEN t1.SC_id_20_28=22 AND t1.qm_new_bki >= 134 AND t1.qm_new_bki < 152 THEN 15
					WHEN t1.SC_id_20_28=22 AND t1.qm_new_bki >= 152 THEN 28
					WHEN t1.SC_id_20_28=22 THEN 5
					WHEN t1.SC_id_20_28=23 AND t1.qm_new_bki = . THEN 12
					WHEN t1.SC_id_20_28=23 AND t1.qm_new_bki < 131 THEN 1
					WHEN t1.SC_id_20_28=23 AND t1.qm_new_bki >= 147 AND t1.qm_new_bki < 159 THEN 22
					WHEN t1.SC_id_20_28=23 AND t1.qm_new_bki >= 159 THEN 27
					WHEN t1.SC_id_20_28=23 THEN 12
					WHEN t1.SC_id_20_28=24 AND t1.qm_new_bki = . THEN 12
					WHEN t1.SC_id_20_28=24 AND t1.qm_new_bki < 131 THEN 0
					WHEN t1.SC_id_20_28=24 AND t1.qm_new_bki >= 131 AND t1.qm_new_bki < 136 THEN 8
					WHEN t1.SC_id_20_28=24 AND t1.qm_new_bki >= 152 AND t1.qm_new_bki < 164 THEN 17
					WHEN t1.SC_id_20_28=24 AND t1.qm_new_bki >= 164 THEN 24
					WHEN t1.SC_id_20_28=24 THEN 12
					WHEN t1.SC_id_20_28=25 AND t1.qm_new_bki = . THEN 28
					WHEN t1.SC_id_20_28=25 AND t1.qm_new_bki < 132 THEN -3
					WHEN t1.SC_id_20_28=25 AND t1.qm_new_bki >= 132 AND t1.qm_new_bki < 143 THEN 6
					WHEN t1.SC_id_20_28=25 AND t1.qm_new_bki >= 143 AND t1.qm_new_bki < 151 THEN 13
					WHEN t1.SC_id_20_28=25 AND t1.qm_new_bki >= 151 AND t1.qm_new_bki < 163 THEN 20
					WHEN t1.SC_id_20_28=25 THEN 28
					WHEN t1.SC_id_20_28=26 AND t1.qm_new_bki = . THEN 23
					WHEN t1.SC_id_20_28=26 AND t1.qm_new_bki < 131 THEN -3
					WHEN t1.SC_id_20_28=26 AND t1.qm_new_bki >= 131 AND t1.qm_new_bki < 143 THEN 5
					WHEN t1.SC_id_20_28=26 AND t1.qm_new_bki >= 143 AND t1.qm_new_bki < 152 THEN 10
					WHEN t1.SC_id_20_28=26 AND t1.qm_new_bki >= 152 AND t1.qm_new_bki < 159 THEN 16
					WHEN t1.SC_id_20_28=26 THEN 23
					WHEN t1.SC_id_20_28=27 AND t1.qm_new_bki = . THEN 18
					WHEN t1.SC_id_20_28=27 AND t1.qm_new_bki < 136 THEN -5
					WHEN t1.SC_id_20_28=27 AND t1.qm_new_bki >= 136 AND t1.qm_new_bki < 146 THEN 4
					WHEN t1.SC_id_20_28=27 AND t1.qm_new_bki >= 164 THEN 39
					WHEN t1.SC_id_20_28=27 THEN 18 
					WHEN t1.SC_id_20_28=28 AND t1.qm_new_bki = . THEN 28
					WHEN t1.SC_id_20_28=28 AND t1.qm_new_bki < 137 THEN 1
					WHEN t1.SC_id_20_28=28 AND t1.qm_new_bki >= 137 AND t1.qm_new_bki < 147 THEN 5
					WHEN t1.SC_id_20_28=28 AND t1.qm_new_bki >= 147 AND t1.qm_new_bki < 152 THEN 12
					WHEN t1.SC_id_20_28=28 AND t1.qm_new_bki >= 152 AND t1.qm_new_bki < 164 THEN 17
					WHEN t1.SC_id_20_28=28 THEN 28 END) as w2_qm_new_bki /*ОК 20-28*/
				,(CASE WHEN t1.SC_id_20_28=26 AND t1.REG_GROUPE_NEW = . THEN 0
					WHEN t1.SC_id_20_28=26 AND t1.REG_GROUPE_NEW = 2 THEN 10
					WHEN t1.SC_id_20_28=26 AND t1.REG_GROUPE_NEW = 1 THEN 15
					WHEN t1.SC_id_20_28=26 THEN 0
					WHEN t1.SC_id_20_28=28 AND t1.REG_GROUPE_NEW = . THEN 1
					WHEN t1.SC_id_20_28=28 AND t1.REG_GROUPE_NEW = 2 THEN 11
					WHEN t1.SC_id_20_28=28 AND t1.REG_GROUPE_NEW = 1 THEN 18
					WHEN t1.SC_id_20_28=28 THEN 1 END) as w2_region_gp /*ОК REGION_GP?!
											REG_GROUPE_NEW*/
				,(CASE WHEN t1.SC_id_20_28=25 AND t1.score_m_sr_simv_veb_bki = . THEN 6
					WHEN t1.SC_id_20_28=25 AND t1.score_m_sr_simv_veb_bki < 10.1 THEN 17
					WHEN t1.SC_id_20_28=25 AND t1.score_m_sr_simv_veb_bki >= 10.1 AND t1.score_m_sr_simv_veb_bki < 11.56 THEN 15
					WHEN t1.SC_id_20_28=25 AND t1.score_m_sr_simv_veb_bki >= 11.56 AND t1.score_m_sr_simv_veb_bki < 13.78 THEN 12
					WHEN t1.SC_id_20_28=25 AND t1.score_m_sr_simv_veb_bki >= 13.78 AND t1.score_m_sr_simv_veb_bki < 17.89 THEN 10
					WHEN t1.SC_id_20_28=25 THEN 6 END) as w2_score_m_sr_simv_veb_bki /*ОК+ 100%*/
				,(CASE WHEN t1.SC_id_20_28=23 AND t1.score_m_sr_simv_veb = . THEN 17
					WHEN t1.SC_id_20_28=23 AND t1.score_m_sr_simv_veb >= 11 AND t1.score_m_sr_simv_veb < 12.25 THEN 11
					WHEN t1.SC_id_20_28=23 AND t1.score_m_sr_simv_veb >= 12.25 THEN 4
					WHEN t1.SC_id_20_28=23 THEN 17 END) as w2_score_m_sr_simv_veb /*ОК. 2015.02.20*/

				,(CASE 
					WHEN t1.SC_id_20_28=25 AND t1.score_n_sr_simv_veb = . THEN 14
					WHEN t1.SC_id_20_28=25 AND t1.score_n_sr_simv_veb < 10.29 THEN 26
					WHEN t1.SC_id_20_28=25 AND t1.score_n_sr_simv_veb >= 10.29 AND t1.score_n_sr_simv_veb < 11.25 THEN 20
					WHEN t1.SC_id_20_28=25 AND t1.score_n_sr_simv_veb >= 13.64 AND t1.score_n_sr_simv_veb < 20.33 THEN 6
					WHEN t1.SC_id_20_28=25 AND t1.score_n_sr_simv_veb >= 20.33 THEN -5
					WHEN t1.SC_id_20_28=25 THEN 14
					WHEN t1.SC_id_20_28=26 AND t1.score_n_sr_simv_veb = . THEN 17
					WHEN t1.SC_id_20_28=26 AND t1.score_n_sr_simv_veb >= 10.32 AND t1.score_n_sr_simv_veb < 11.33 THEN 12
					WHEN t1.SC_id_20_28=26 AND t1.score_n_sr_simv_veb >= 11.33 AND t1.score_n_sr_simv_veb < 14.94 THEN 9
					WHEN t1.SC_id_20_28=26 AND t1.score_n_sr_simv_veb >= 14.94 THEN 4
					WHEN t1.SC_id_20_28=26 THEN 17
					WHEN t1.SC_id_20_28=28 AND t1.score_n_sr_simv_veb = . THEN 8
					WHEN t1.SC_id_20_28=28 AND t1.score_n_sr_simv_veb < 10.37 THEN 19
					WHEN t1.SC_id_20_28=28 AND t1.score_n_sr_simv_veb >= 10.37 AND t1.score_n_sr_simv_veb < 10.95 THEN 14
					WHEN t1.SC_id_20_28=28 AND t1.score_n_sr_simv_veb >= 10.95 AND t1.score_n_sr_simv_veb < 14.42 THEN 11
					WHEN t1.SC_id_20_28=28 THEN 8 END) as w2_score_n_sr_simv_veb /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.score_n_sr_simv_veb_bki = . THEN 8
					WHEN t1.SC_id_20_28=20 AND t1.score_n_sr_simv_veb_bki < 10.1 THEN 10
					WHEN t1.SC_id_20_28=20 AND t1.score_n_sr_simv_veb_bki >= 10.1 AND t1.score_n_sr_simv_veb_bki < 12.2 THEN 9
					WHEN t1.SC_id_20_28=20 AND t1.score_n_sr_simv_veb_bki >= 12.2 AND t1.score_n_sr_simv_veb_bki < 14 THEN 8
					WHEN t1.SC_id_20_28=20 AND t1.score_n_sr_simv_veb_bki >= 14 THEN 7
					WHEN t1.SC_id_20_28=20 THEN 8
					WHEN t1.SC_id_20_28=21 AND t1.score_n_sr_simv_veb_bki = . THEN 13
					WHEN t1.SC_id_20_28=21 AND t1.score_n_sr_simv_veb_bki < 10.25 THEN 7
					WHEN t1.SC_id_20_28=21 AND t1.score_n_sr_simv_veb_bki >= 10.25 AND t1.score_n_sr_simv_veb_bki < 11.74 THEN 9
					WHEN t1.SC_id_20_28=21 AND t1.score_n_sr_simv_veb_bki >= 11.74 THEN 14
					WHEN t1.SC_id_20_28=21 THEN 13
					WHEN t1.SC_id_20_28=22 AND t1.score_n_sr_simv_veb_bki = . THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.score_n_sr_simv_veb_bki < 10.5 THEN 10
					WHEN t1.SC_id_20_28=22 AND t1.score_n_sr_simv_veb_bki >= 10.5 AND t1.score_n_sr_simv_veb_bki < 11.43 THEN 11
					WHEN t1.SC_id_20_28=22 THEN 12 END) as w2_score_n_sr_simv_veb_bki /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=24 AND t1.REG_GROUPE_NEW = . THEN 3
					WHEN t1.SC_id_20_28=24 AND t1.REG_GROUPE_NEW = 1 THEN 16
					WHEN t1.SC_id_20_28=24 AND t1.REG_GROUPE_NEW = 2 THEN 11
					WHEN t1.SC_id_20_28=24 THEN 3 END) as w2_scorecard_region_group_v2p /*ОК scorecard_region_group_v2p?! 
							REG_GROUPE_NEW*/
				,(CASE WHEN t1.SC_id_20_28=24 AND t1.sd_20_28 = . THEN 13
					WHEN t1.SC_id_20_28=24 AND t1.sd_20_28 >= 0.02 AND t1.sd_20_28 < 0.04 THEN 11
					WHEN t1.SC_id_20_28=24 AND t1.sd_20_28 >= 0.04 AND t1.sd_20_28 < 0.07 THEN 9
					WHEN t1.SC_id_20_28=24 AND t1.sd_20_28 >= 0.07 THEN 6
					WHEN t1.SC_id_20_28=24 THEN 13
					WHEN t1.SC_id_20_28=25 AND t1.sd_20_28 = . THEN 14
					WHEN t1.SC_id_20_28=25 AND t1.sd_20_28 < 0.02 THEN 15
					WHEN t1.SC_id_20_28=25 AND t1.sd_20_28 >= 0.04 AND t1.sd_20_28 < 0.06 THEN 13
					WHEN t1.SC_id_20_28=25 AND t1.sd_20_28 >= 0.06 THEN 11
					WHEN t1.SC_id_20_28=25 THEN 14
					WHEN t1.SC_id_20_28=26 AND t1.sd_20_28 = . THEN 15
					WHEN t1.SC_id_20_28=26 AND t1.sd_20_28 >= 0.04 AND t1.sd_20_28 < 0.06 THEN 11
					WHEN t1.SC_id_20_28=26 AND t1.sd_20_28 >= 0.06 AND t1.sd_20_28 < 0.09 THEN 8
					WHEN t1.SC_id_20_28=26 AND t1.sd_20_28 >= 0.09 THEN 5
					WHEN t1.SC_id_20_28=26 THEN 15
					WHEN t1.SC_id_20_28=27 AND t1.sd_20_28 = . THEN 17
					WHEN t1.SC_id_20_28=27 AND t1.sd_20_28 >= 0.01 AND t1.sd_20_28 < 0.03 THEN 15
					WHEN t1.SC_id_20_28=27 AND t1.sd_20_28 >= 0.03 THEN 12
					WHEN t1.SC_id_20_28=27 THEN 17
					WHEN t1.SC_id_20_28=28 AND t1.sd_20_28 = . THEN 18
					WHEN t1.SC_id_20_28=28 AND t1.sd_20_28 >= 0.01 AND t1.sd_20_28 < 0.02 THEN 15
					WHEN t1.SC_id_20_28=28 AND t1.sd_20_28 >= 0.02 AND t1.sd_20_28 < 0.03 THEN 12
					WHEN t1.SC_id_20_28=28 AND t1.sd_20_28 >= 0.03 THEN 8
					WHEN t1.SC_id_20_28=28 THEN 18 END) as w2_sd /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.sex_age_derivative = . THEN 10
					WHEN t1.SC_id_20_28=20 AND t1.sex_age_derivative < 31 THEN 7
					WHEN t1.SC_id_20_28=20 AND t1.sex_age_derivative >= 31 AND t1.sex_age_derivative < 56 THEN 9
					WHEN t1.SC_id_20_28=20 THEN 10
					WHEN t1.SC_id_20_28=21 AND t1.sex_age_derivative = . THEN 7
					WHEN t1.SC_id_20_28=21 AND t1.sex_age_derivative >= 88 AND t1.sex_age_derivative < 106 THEN 11
					WHEN t1.SC_id_20_28=21 AND t1.sex_age_derivative >= 106 THEN 15
					WHEN t1.SC_id_20_28=21 THEN 7
					WHEN t1.SC_id_20_28=22 AND t1.sex_age_derivative = . THEN 9
					WHEN t1.SC_id_20_28=22 AND t1.sex_age_derivative < 46 THEN 5
					WHEN t1.SC_id_20_28=22 AND t1.sex_age_derivative >= 88 AND t1.sex_age_derivative < 104 THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.sex_age_derivative >= 104 AND t1.sex_age_derivative < 120 THEN 16
					WHEN t1.SC_id_20_28=22 AND t1.sex_age_derivative >= 120 THEN 20
					WHEN t1.SC_id_20_28=22 THEN 9
					WHEN t1.SC_id_20_28=23 AND t1.sex_age_derivative = . THEN 8
					WHEN t1.SC_id_20_28=23 AND t1.sex_age_derivative < 35 THEN 1
					WHEN t1.SC_id_20_28=23 AND t1.sex_age_derivative >= 59 AND t1.sex_age_derivative < 92 THEN 13
					WHEN t1.SC_id_20_28=23 AND t1.sex_age_derivative >= 92 AND t1.sex_age_derivative < 124 THEN 23
					WHEN t1.SC_id_20_28=23 AND t1.sex_age_derivative >= 124 THEN 34
					WHEN t1.SC_id_20_28=23 THEN 8
					WHEN t1.SC_id_20_28=24 AND t1.sex_age_derivative = . THEN 9
					WHEN t1.SC_id_20_28=24 AND t1.sex_age_derivative < 46 THEN 6
					WHEN t1.SC_id_20_28=24 AND t1.sex_age_derivative >= 84 AND t1.sex_age_derivative < 108 THEN 13
					WHEN t1.SC_id_20_28=24 AND t1.sex_age_derivative >= 108 THEN 17
					WHEN t1.SC_id_20_28=24 THEN 9
					WHEN t1.SC_id_20_28=25 AND t1.sex_age_derivative = . THEN 13
					WHEN t1.SC_id_20_28=25 AND t1.sex_age_derivative < 31 THEN 4
					WHEN t1.SC_id_20_28=25 AND t1.sex_age_derivative >= 31 AND t1.sex_age_derivative < 38 THEN 8
					WHEN t1.SC_id_20_28=25 AND t1.sex_age_derivative >= 70 AND t1.sex_age_derivative < 82 THEN 18
					WHEN t1.SC_id_20_28=25 AND t1.sex_age_derivative >= 82 THEN 23
					WHEN t1.SC_id_20_28=25 THEN 13
					WHEN t1.SC_id_20_28=27 AND t1.sex_age_derivative = . THEN 22
					WHEN t1.SC_id_20_28=27 AND t1.sex_age_derivative < 100 THEN 1
					WHEN t1.SC_id_20_28=27 AND t1.sex_age_derivative >= 100 AND t1.sex_age_derivative < 108 THEN 12
					WHEN t1.SC_id_20_28=27 AND t1.sex_age_derivative >= 108 AND t1.sex_age_derivative < 116 THEN 18
					WHEN t1.SC_id_20_28=27 AND t1.sex_age_derivative >= 128 THEN 27
					WHEN t1.SC_id_20_28=27 THEN 22 END) as w2_sex_age_derivative /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=23 AND t1.sko_12 = . THEN 15
					WHEN t1.SC_id_20_28=23 AND t1.sko_12 >= 4.92 AND t1.sko_12 < 6.69 THEN 13
					WHEN t1.SC_id_20_28=23 AND t1.sko_12 >= 6.69 THEN 11
					WHEN t1.SC_id_20_28=23 THEN 15 END) as w2_sko_12 /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=23 AND t1.speeddynam_12 = . THEN 14
					WHEN t1.SC_id_20_28=23 AND t1.speeddynam_12 < -0.8 THEN 11
					WHEN t1.SC_id_20_28=23 AND t1.speeddynam_12 >= 0 AND t1.speeddynam_12 < 0.59 THEN 15
					WHEN t1.SC_id_20_28=23 AND t1.speeddynam_12 >= 0.59 THEN 10
					WHEN t1.SC_id_20_28=23 THEN 14
					WHEN t1.SC_id_20_28=25 AND t1.speeddynam_12 = . THEN 15
					WHEN t1.SC_id_20_28=25 AND t1.speeddynam_12 < -1.4 THEN 9
					WHEN t1.SC_id_20_28=25 AND t1.speeddynam_12 >= -0.38  AND t1.speeddynam_12<0.31 THEN 13
					WHEN t1.SC_id_20_28=25 AND t1.speeddynam_12 >= 0.31 THEN 10
					WHEN t1.SC_id_20_28=25 THEN 15
					WHEN t1.SC_id_20_28=27 AND t1.speeddynam_12 = . THEN 18
					WHEN t1.SC_id_20_28=27 AND t1.speeddynam_12 < -1.12 THEN 8
					WHEN t1.SC_id_20_28=27 AND t1.speeddynam_12 >= -1.12 and speeddynam_12 < -0.73 THEN 22
					WHEN t1.SC_id_20_28=27 AND t1.speeddynam_12 >= 0.31 THEN 7
					WHEN t1.SC_id_20_28=27 THEN 18 END) as w2_speeddynam_12 /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.stag_in_months = . THEN 9
					WHEN t1.SC_id_20_28=20 AND t1.stag_in_months >= 6 AND t1.stag_in_months < 30 THEN 7
					WHEN t1.SC_id_20_28=20 AND t1.stag_in_months >= 30 AND t1.stag_in_months < 48 THEN 8
					WHEN t1.SC_id_20_28=20 AND t1.stag_in_months >= 48 AND t1.stag_in_months < 84 THEN 12
					WHEN t1.SC_id_20_28=20 AND t1.stag_in_months >= 84 THEN 15
					WHEN t1.SC_id_20_28=20 THEN 9
					WHEN t1.SC_id_20_28=21 AND t1.stag_in_months = . THEN 6
					WHEN t1.SC_id_20_28=21 AND t1.stag_in_months < 5 THEN 12
					WHEN t1.SC_id_20_28=21 AND t1.stag_in_months >= 60 AND t1.stag_in_months < 120 THEN 11
					WHEN t1.SC_id_20_28=21 AND t1.stag_in_months >= 120 THEN 16
					WHEN t1.SC_id_20_28=21 THEN 6
					WHEN t1.SC_id_20_28=22 AND t1.stag_in_months = . THEN 5
					WHEN t1.SC_id_20_28=22 AND t1.stag_in_months < 3 THEN 14
					WHEN t1.SC_id_20_28=22 AND t1.stag_in_months >= 24 AND t1.stag_in_months < 60 THEN 7
					WHEN t1.SC_id_20_28=22 AND t1.stag_in_months >= 60 AND t1.stag_in_months < 96 THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.stag_in_months >= 96 THEN 18
					WHEN t1.SC_id_20_28=22 THEN 5
					WHEN t1.SC_id_20_28=24 AND t1.stag_in_months = . THEN 8
					WHEN t1.SC_id_20_28=24 AND t1.stag_in_months < 3 THEN 11
					WHEN t1.SC_id_20_28=24 AND t1.stag_in_months >= 59 AND t1.stag_in_months < 120 THEN 12
					WHEN t1.SC_id_20_28=24 AND t1.stag_in_months >= 120 THEN 16
					WHEN t1.SC_id_20_28=24 THEN 8
					WHEN t1.SC_id_20_28=25 AND t1.stag_in_months = . THEN 9
					WHEN t1.SC_id_20_28=25 AND t1.stag_in_months >= 36 AND t1.stag_in_months < 60 THEN 12
					WHEN t1.SC_id_20_28=25 AND t1.stag_in_months >= 60 AND t1.stag_in_months < 96 THEN 16
					WHEN t1.SC_id_20_28=25 AND t1.stag_in_months >= 96 AND t1.stag_in_months < 156 THEN 18
					WHEN t1.SC_id_20_28=25 AND t1.stag_in_months >= 156 THEN 23
					WHEN t1.SC_id_20_28=25 THEN 9
					WHEN t1.SC_id_20_28=26 AND t1.stag_in_months = . THEN 8
					WHEN t1.SC_id_20_28=26 AND t1.stag_in_months >= 36 AND t1.stag_in_months < 60 THEN 10
					WHEN t1.SC_id_20_28=26 AND t1.stag_in_months >= 60 AND t1.stag_in_months < 108 THEN 14
					WHEN t1.SC_id_20_28=26 AND t1.stag_in_months >= 108 THEN 19
					WHEN t1.SC_id_20_28=26 THEN 8 END) as w2_stag_in_months /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=26 AND t1.sum_12_rate = . THEN 15
					WHEN t1.SC_id_20_28=26 AND t1.sum_12_rate >= 120 AND t1.sum_12_rate < 130 THEN 14
					WHEN t1.SC_id_20_28=26 AND t1.sum_12_rate >= 130 AND t1.sum_12_rate < 150 THEN 11
					WHEN t1.SC_id_20_28=26 AND t1.sum_12_rate >= 150 AND t1.sum_12_rate < 190 THEN 10
					WHEN t1.SC_id_20_28=26 AND t1.sum_12_rate >= 190 THEN 7
					WHEN t1.SC_id_20_28=26 THEN 15
					WHEN t1.SC_id_20_28=28 AND t1.sum_12_rate = . THEN 17
					WHEN t1.SC_id_20_28=28 AND t1.sum_12_rate >= 120 AND t1.sum_12_rate < 130 THEN 15
					WHEN t1.SC_id_20_28=28 AND t1.sum_12_rate >= 130 AND t1.sum_12_rate < 150 THEN 13
					WHEN t1.SC_id_20_28=28 AND t1.sum_12_rate >= 150 THEN 11
					WHEN t1.SC_id_20_28=28 THEN 17 END) as w2_sum_12_rate /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=24 AND t1.sum_6_rate = . THEN 11
					WHEN t1.SC_id_20_28=24 AND t1.sum_6_rate < 60 THEN 18
					WHEN t1.SC_id_20_28=24 AND t1.sum_6_rate >= 70 THEN 1
					WHEN t1.SC_id_20_28=24 THEN 11 END) as w2_sum_6_rate /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=25 AND t1.summ_aggregate = . THEN 14
					WHEN t1.SC_id_20_28=25 AND t1.summ_aggregate < 30000 THEN 24
					WHEN t1.SC_id_20_28=25 AND t1.summ_aggregate >= 71000 AND t1.summ_aggregate < 97000 THEN 13
					WHEN t1.SC_id_20_28=25 AND t1.summ_aggregate >= 97000 THEN 4
					WHEN t1.SC_id_20_28=25 THEN 14
					WHEN t1.SC_id_20_28=26 AND t1.summ_aggregate = . THEN 17
					WHEN t1.SC_id_20_28=26 AND t1.summ_aggregate < 145000 THEN 26
					WHEN t1.SC_id_20_28=26 AND t1.summ_aggregate >= 230000 AND t1.summ_aggregate < 315000 THEN 13
					WHEN t1.SC_id_20_28=26 AND t1.summ_aggregate >= 315000 AND t1.summ_aggregate < 520000 THEN 7
					WHEN t1.SC_id_20_28=26 AND t1.summ_aggregate >= 520000 THEN -1
					WHEN t1.SC_id_20_28=26 THEN 17
					WHEN t1.SC_id_20_28=28 AND t1.summ_aggregate = . THEN 12
					WHEN t1.SC_id_20_28=28 AND t1.summ_aggregate < 170000 THEN 29
					WHEN t1.SC_id_20_28=28 AND t1.summ_aggregate >= 170000 AND t1.summ_aggregate < 260000 THEN 20
					WHEN t1.SC_id_20_28=28 AND t1.summ_aggregate >= 430000 AND t1.summ_aggregate < 550000 THEN 4
					WHEN t1.SC_id_20_28=28 AND t1.summ_aggregate >= 550000 THEN -2
					WHEN t1.SC_id_20_28=28 THEN 12 END) as w2_summ_aggregate /*ОК*/
				,(CASE WHEN t1.SC_id_20_28=20 AND t1.type_client = . THEN 6
					WHEN t1.SC_id_20_28=20 AND t1.type_client = 3 THEN 10
					WHEN t1.SC_id_20_28=20 AND t1.type_client IN(1,4) THEN 14
					WHEN t1.SC_id_20_28=20 THEN 6
					WHEN t1.SC_id_20_28=21 AND t1.type_client = . THEN 11
					WHEN t1.SC_id_20_28=21 AND t1.type_client IN(1,4) THEN 6
					WHEN t1.SC_id_20_28=21 AND t1.type_client = 3 THEN 7
					WHEN t1.SC_id_20_28=21 THEN 11
					WHEN t1.SC_id_20_28=22 AND t1.type_client = . THEN 12
					WHEN t1.SC_id_20_28=22 AND t1.type_client IN(1,3,4) THEN 9
					WHEN t1.SC_id_20_28=22 THEN 12 END) as w2_type_client /*ОК*/
					
				,(CASE
				/*вычисляем значение веса параметра v_antiquity_veb_bki  */
					WHEN antiquity_veb_bki = . THEN 28
					WHEN antiquity_veb_bki < 1 THEN 23
					WHEN antiquity_veb_bki >= 1 AND antiquity_veb_bki < 3 THEN 28
					WHEN antiquity_veb_bki >= 3 AND antiquity_veb_bki < 6 THEN 47
					WHEN antiquity_veb_bki >= 6 AND antiquity_veb_bki < 14 THEN 51
					WHEN antiquity_veb_bki >= 14 THEN 40
					else 28 end) as v_antiquity_veb_bki
				/*далее суммируем вычисляем значение веса параметра v_defolt_veb_bki (показатель выхода в дефолт по дан-ным БКИ) */
				,(CASE
					WHEN DEFOLT_veb_bki = . THEN 30
					WHEN DEFOLT_veb_bki>=1 THEN 14
					else 30 end) as v_defolt_veb_bki
				/*суммируем значение веса параметра v_duration_veb_bki (продолжительность КИ по данным БКИ (количество месяцев КИ заявителя) */
				,(CASE
					WHEN duration_veb_bki = . THEN 31
					WHEN duration_veb_bki < 7 THEN 1
					WHEN duration_veb_bki >= 7 AND duration_veb_bki < 22 THEN 15
					WHEN duration_veb_bki >= 22 AND duration_veb_bki < 41 THEN 31
					WHEN duration_veb_bki >= 41 AND duration_veb_bki < 60 THEN 43
					WHEN duration_veb_bki >= 60 THEN 51
					ELSE 31 END) as v_duration_veb_bki
				/*вычисляем значение веса компоненты dynamics_quality_bki (динамика модели качества по данным БКИ) как v_dynamics_quality_bki*/
				,(CASE
					WHEN dynamics_quality_bki=. THEN 24
					WHEN dynamics_quality_bki< 0.71 THEN 22
					WHEN dynamics_quality_bki>= 0.71 AND dynamics_quality_bki< 1 THEN 31
					WHEN dynamics_quality_bki>= 1 AND dynamics_quality_bki< 1.01 THEN 30
					WHEN dynamics_quality_bki>= 1.01 AND dynamics_quality_bki< 1.07 THEN 27
					WHEN dynamics_quality_bki>=1.07 THEN 24
					else 24 end) as v_dynamics_quality_bki
				/* далее суммируем значение веса v_score_n_sr_simv_veb
				(средний балл сроки качества КИ клиента за период n по данным ВЭБ) */
				,(CASE
					WHEN score_n_sr_simv_veb_bki=. THEN 23
					WHEN score_n_sr_simv_veb_bki< 10.25 THEN 33
					WHEN score_n_sr_simv_veb_bki>= 10.25 AND score_n_sr_simv_veb_bki< 11.13 THEN 40
					WHEN score_n_sr_simv_veb_bki>= 11.13 AND score_n_sr_simv_veb_bki< 15.83 THEN 23
					WHEN score_n_sr_simv_veb_bki>= 15.83 THEN 11
					else 23 end) as v_score_n_sr_simv_bki
				/*веса для карт 9 поколения от 04.2015 */
				,(CASE
						WHEN t1.SC_id_30_38=30 AND CNT_REJECTS = . THEN 12
						WHEN t1.SC_id_30_38=30 AND CNT_REJECTS < 1 THEN 11
						WHEN t1.SC_id_30_38=30 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 4
						WHEN t1.SC_id_30_38=30 AND CNT_REJECTS >= 2 THEN 0
						WHEN t1.SC_id_30_38=31 AND CNT_REJECTS = . THEN 15
						WHEN t1.SC_id_30_38=31 AND CNT_REJECTS < 1 THEN 12
						WHEN t1.SC_id_30_38=31 AND CNT_REJECTS >= 1 THEN -2
						WHEN t1.SC_id_30_38=32 AND CNT_REJECTS = . THEN 16
						WHEN t1.SC_id_30_38=32 AND CNT_REJECTS < 1 THEN 14
						WHEN t1.SC_id_30_38=32 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 7
						WHEN t1.SC_id_30_38=32 AND CNT_REJECTS >= 2 THEN 2
						/*
						WHEN t1.SC_id_30_38=33 AND CNT_REJECTS = . THEN 11
						WHEN t1.SC_id_30_38=33 AND CNT_REJECTS < 1 THEN 23
						WHEN t1.SC_id_30_38=33 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 11
						WHEN t1.SC_id_30_38=33 AND CNT_REJECTS >= 2 THEN -2
						*/
						WHEN t1.SC_id_30_38=33 AND CNT_REJECTS = . THEN 12
						WHEN t1.SC_id_30_38=33 AND CNT_REJECTS < 1 THEN 24
						WHEN t1.SC_id_30_38=33 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 12
						WHEN t1.SC_id_30_38=33 AND CNT_REJECTS >= 2 THEN 0
						WHEN t1.SC_id_30_38=34 AND CNT_REJECTS = . THEN 20
						WHEN t1.SC_id_30_38=34 AND CNT_REJECTS < 1 THEN 20
						WHEN t1.SC_id_30_38=34 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 10
						WHEN t1.SC_id_30_38=34 AND CNT_REJECTS >= 2 AND CNT_REJECTS < 3 THEN 6
						WHEN t1.SC_id_30_38=34 AND CNT_REJECTS >= 3 THEN -1
						WHEN t1.SC_id_30_38=35 AND CNT_REJECTS = . THEN 12
						WHEN t1.SC_id_30_38=35 AND CNT_REJECTS < 1 THEN 23
						WHEN t1.SC_id_30_38=35 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 15
						WHEN t1.SC_id_30_38=35 AND CNT_REJECTS >= 2 AND CNT_REJECTS < 3 THEN 12
						WHEN t1.SC_id_30_38=35 AND CNT_REJECTS >= 3 THEN 5
						WHEN t1.SC_id_30_38=36 AND CNT_REJECTS = . THEN 14
						WHEN t1.SC_id_30_38=36 AND CNT_REJECTS < 1 THEN 22
						WHEN t1.SC_id_30_38=36 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 14
						WHEN t1.SC_id_30_38=36 AND CNT_REJECTS >= 2 AND CNT_REJECTS < 4 THEN 8
						WHEN t1.SC_id_30_38=36 AND CNT_REJECTS >= 4 THEN -1
						/*
						WHEN t1.SC_id_30_38=37 AND CNT_REJECTS = . THEN 13
						WHEN t1.SC_id_30_38=37 AND CNT_REJECTS < 1 THEN 27
						WHEN t1.SC_id_30_38=37 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 18
						WHEN t1.SC_id_30_38=37 AND CNT_REJECTS >= 2 THEN 7
						*/
						WHEN t1.SC_id_30_38=37 AND CNT_REJECTS = . THEN 10
						WHEN t1.SC_id_30_38=37 AND CNT_REJECTS < 1 THEN 28
						WHEN t1.SC_id_30_38=37 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 19
						WHEN t1.SC_id_30_38=37 AND CNT_REJECTS >= 2 THEN 10
						WHEN t1.SC_id_30_38=38 AND CNT_REJECTS = . THEN 17
						WHEN t1.SC_id_30_38=38 AND CNT_REJECTS < 1 THEN 26
						WHEN t1.SC_id_30_38=38 AND CNT_REJECTS >= 1 AND CNT_REJECTS < 2 THEN 17
						WHEN t1.SC_id_30_38=38 AND CNT_REJECTS >= 2 THEN 6
					END) AS Wg9_CNT_REJECTS
				, (CASE
						WHEN t1.SC_id_30_38=30 AND CR_HIST_BKI = . THEN 8
						WHEN t1.SC_id_30_38=30 AND CR_HIST_BKI < 8 THEN 13
						WHEN t1.SC_id_30_38=30 AND CR_HIST_BKI >= 8 AND CR_HIST_BKI < 16 THEN 10
						WHEN t1.SC_id_30_38=30 AND CR_HIST_BKI >= 16 AND CR_HIST_BKI < 56 THEN 8
						WHEN t1.SC_id_30_38=30 AND CR_HIST_BKI >=56 THEN 7
						/*
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI = . THEN 17
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI < 10 THEN 13
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI >= 10 AND CR_HIST_BKI < 38 THEN 14
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI >= 18 AND CR_HIST_BKI < 33 THEN 15
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI >=33 THEN 17
						*/
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI = . THEN 18
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI < 10 THEN 14
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI >= 10 AND CR_HIST_BKI < 18 THEN 15
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI >= 18 AND CR_HIST_BKI < 33 THEN 17
						WHEN t1.SC_id_30_38=33 AND CR_HIST_BKI >=33 THEN 18
					END) AS Wg9_CR_HIST_BKI
				, (CASE
						WHEN t1.SC_id_30_38=30 AND D = . THEN 7
						WHEN t1.SC_id_30_38=30 AND D < 0.2 THEN 11
						WHEN t1.SC_id_30_38=30 AND D >= 0.2 AND D < 0.24 THEN 9
						WHEN t1.SC_id_30_38=30 AND D >= 0.24 AND D < 0.28 THEN 8
						WHEN t1.SC_id_30_38=30 AND D >= 0.28 THEN 7
						WHEN t1.SC_id_30_38=31 AND D = . THEN 11
						WHEN t1.SC_id_30_38=31 AND D < 0.12 THEN 13
						WHEN t1.SC_id_30_38=31 AND D >= 0.12 AND D < 0.17 THEN 11
						WHEN t1.SC_id_30_38=31 AND D >= 0.17 THEN 10
						WHEN t1.SC_id_30_38=32 AND D = . THEN 13
						WHEN t1.SC_id_30_38=32 AND D < 0.1 THEN 14
						WHEN t1.SC_id_30_38=32 AND D >= 0.1 AND D < 0.14 THEN 13
						WHEN t1.SC_id_30_38=32 AND D >= 0.14 AND D < 0.19 THEN 12
						WHEN t1.SC_id_30_38=32 AND D >= 0.19 THEN 10
						/*
						WHEN t1.SC_id_30_38=33 AND D = . THEN 17
						WHEN t1.SC_id_30_38=33 AND D < 0.11 THEN 23
						WHEN t1.SC_id_30_38=33 AND D >= 0.11 AND D < 0.18 THEN 17
						WHEN t1.SC_id_30_38=33 AND D >= 0.18 AND D < 0.26 THEN 9
						WHEN t1.SC_id_30_38=33 AND D >= 0.26 THEN 5
						*/
						WHEN t1.SC_id_30_38=33 AND D = . THEN 18
						WHEN t1.SC_id_30_38=33 AND D < 0.11 THEN 24
						WHEN t1.SC_id_30_38=33 AND D >= 0.11 AND D < 0.18 THEN 18
						WHEN t1.SC_id_30_38=33 AND D >= 0.18 AND D < 0.26 THEN 10
						WHEN t1.SC_id_30_38=33 AND D >= 0.26 THEN 6
					END) AS wG9_D
				, (CASE
						WHEN t1.SC_id_30_38=30 AND delay_day_y_bki = . THEN 7
						WHEN t1.SC_id_30_38=30 AND delay_day_y_bki < 1 THEN 12
						WHEN t1.SC_id_30_38=30 AND delay_day_y_bki >= 1 THEN 7
						WHEN t1.SC_id_30_38=31 AND delay_day_y_bki = . THEN 8
						WHEN t1.SC_id_30_38=31 AND delay_day_y_bki < 1 THEN 14
						WHEN t1.SC_id_30_38=31 AND delay_day_y_bki >= 1 THEN 8
						WHEN t1.SC_id_30_38=32 AND delay_day_y_bki = . THEN 10
						WHEN t1.SC_id_30_38=32 AND delay_day_y_bki < 1 THEN 15
						WHEN t1.SC_id_30_38=32 AND delay_day_y_bki >= 1 THEN 8
					END) AS wG9_delay_day_y_bki
				, (CASE
						WHEN t1.SC_id_30_38=30 AND EDUCATION = . THEN 7
						WHEN t1.SC_id_30_38=30 AND EDUCATION IN(4,6) THEN 0
						WHEN t1.SC_id_30_38=30 AND EDUCATION IN(1,5) THEN 7
						WHEN t1.SC_id_30_38=30 AND EDUCATION = 3 THEN 13
						WHEN t1.SC_id_30_38=30 AND EDUCATION = 2 THEN 22
						WHEN t1.SC_id_30_38=30 THEN EDUCATION = 7
						WHEN t1.SC_id_30_38=31 AND EDUCATION = . THEN 8
						WHEN t1.SC_id_30_38=31 AND EDUCATION IN(1,3,4,6) THEN 8
						WHEN t1.SC_id_30_38=31 AND EDUCATION = 5 THEN 12
						WHEN t1.SC_id_30_38=31 AND EDUCATION = 2 THEN 20
						WHEN t1.SC_id_30_38=31 THEN EDUCATION = 8
						WHEN t1.SC_id_30_38=35 AND EDUCATION = . THEN 27
						WHEN t1.SC_id_30_38=35 AND EDUCATION IN(4,6) THEN 8
						WHEN t1.SC_id_30_38=35 AND EDUCATION = 5 THEN 14
						WHEN t1.SC_id_30_38=35 AND EDUCATION IN(1,2,3) THEN 27
						WHEN t1.SC_id_30_38=35 THEN EDUCATION = 27
					END) AS wG9_EDUCATION
				, (CASE
						WHEN t1.SC_id_30_38=30 AND HOUSEHOLD = . THEN 4
						WHEN t1.SC_id_30_38=30 AND HOUSEHOLD IN(1,10,3) THEN 4
						WHEN t1.SC_id_30_38=30 AND HOUSEHOLD IN(11,12,2,4,6) THEN 9
						WHEN t1.SC_id_30_38=30 AND HOUSEHOLD IN(5,8,9) THEN 11
						WHEN t1.SC_id_30_38=30 AND HOUSEHOLD = 7 THEN 13
						WHEN t1.SC_id_30_38=30 THEN HOUSEHOLD = 4
						WHEN t1.SC_id_30_38=32 AND HOUSEHOLD = . THEN 5
						WHEN t1.SC_id_30_38=32 AND HOUSEHOLD IN(1,2,3) THEN 5
						WHEN t1.SC_id_30_38=32 AND HOUSEHOLD IN(10,11,15) THEN 9
						WHEN t1.SC_id_30_38=32 AND HOUSEHOLD IN(12,7,8) THEN 16
						WHEN t1.SC_id_30_38=32 AND HOUSEHOLD IN(4,6,9) THEN 18
						WHEN t1.SC_id_30_38=32 THEN HOUSEHOLD = 5
						WHEN t1.SC_id_30_38=35 AND HOUSEHOLD = . THEN 20
						WHEN t1.SC_id_30_38=35 AND HOUSEHOLD IN(2,3) THEN 10
						WHEN t1.SC_id_30_38=35 AND HOUSEHOLD IN(1,5,8) THEN 12
						WHEN t1.SC_id_30_38=35 AND HOUSEHOLD IN(10,4,6) THEN 17
						WHEN t1.SC_id_30_38=35 AND HOUSEHOLD IN(11,12,7,9) THEN 20
						WHEN t1.SC_id_30_38=35 THEN HOUSEHOLD = 20
					END) AS wG9_HOUSEHOLD
				, (CASE
						WHEN t1.SC_id_30_38=30 AND ORG_VID = . THEN 2
						WHEN t1.SC_id_30_38=30 AND ORG_VID IN(25,28,30,31,33,45,51,53,55,58,62,65,67,68,69,84,98,99) THEN 5
						WHEN t1.SC_id_30_38=30 AND ORG_VID IN(100,101,103,22,24,26,35,38,42,43,47,50,52,54,59,61,66,70,80,81,95) THEN 9
						WHEN t1.SC_id_30_38=30 AND ORG_VID IN(0,105,19,21,23,27,40,46,49,57,63,64,71,72,73,74,75,76,77,79,82,85,86) THEN 15
						WHEN t1.SC_id_30_38=30 AND ORG_VID IN(104,17,18,20,39,56,60,78,83,88,89,90,91,92,93,94,96,97) THEN 27
						WHEN t1.SC_id_30_38=30 AND ORG_VID IN(102,107,29,32,34,36,37,41,44,48,87) THEN 2
						WHEN t1.SC_id_30_38=30 THEN 2
						WHEN t1.SC_id_30_38=31 AND ORG_VID = . THEN 5
						WHEN t1.SC_id_30_38=31 AND ORG_VID IN(31,33,35,41,43,48,50,51,52,55,56,81,88) THEN 8
						WHEN t1.SC_id_30_38=31 AND ORG_VID IN(105,22,23,24,25,30,32,54,62,66,67,69,70,79,80,85,95,97,98,99) THEN 10
						WHEN t1.SC_id_30_38=31 AND ORG_VID IN(102,21,26,27,37,38,42,45,46,49,58,59,65,71,73,84) THEN 13
						WHEN t1.SC_id_30_38=31 AND ORG_VID IN(100,101,107,17,18,19,39,40,47,61,63,64,72,74,75,76,78,82,83,87,90,91,92,93,94,96) THEN 16
						WHEN t1.SC_id_30_38=31 AND ORG_VID IN(0,103,20,28,29,34,36,44,53,57,60,68,77,86,89) THEN 5
						WHEN t1.SC_id_30_38=31 THEN 5
						WHEN t1.SC_id_30_38=32 AND ORG_VID = . THEN 4
						WHEN t1.SC_id_30_38=32 AND ORG_VID IN(0,103,107,29,30,34,36,41,44,48,53,67,68,98) THEN 4
						WHEN t1.SC_id_30_38=32 AND ORG_VID IN(20,28,31,33,35,43,47,49,51,55,56,69,99) THEN 7
						WHEN t1.SC_id_30_38=32 AND ORG_VID IN(105,22,23,24,25,32,38,42,45,46,50,54,58,62,63,65,66,70,75,81,84,87,95) THEN 11
						WHEN t1.SC_id_30_38=32 AND ORG_VID IN(100,101,102,21,26,27,37,57,60,61,64,71,73,76,79,88,89,90) THEN 18
						WHEN t1.SC_id_30_38=32 AND ORG_VID IN(17,18,19,39,40,52,59,72,74,77,78,80,82,83,85,86,91,92,93,94,96,97) THEN 22
						WHEN t1.SC_id_30_38=32 THEN 4
						/*
						WHEN t1.SC_id_30_38=33 AND ORG_VID = . THEN 26
						WHEN t1.SC_id_30_38=33 AND ORG_VID IN(0,24,28,29,30,31,32,33,34,35,36,39,42,44,58,59,63,67,68,80,81,88,96,98) THEN 3
						WHEN t1.SC_id_30_38=33 AND ORG_VID IN(22,23,25,41,43,45,48,50,51,53,54,55,62,66,69,70,76,99) THEN 11
						WHEN t1.SC_id_30_38=33 AND ORG_VID IN(102,103,105,21,38,46,57,71,75,78,91,93,95) THEN 17
						WHEN t1.SC_id_30_38=33 AND ORG_VID IN(100,101,104,17,18,19,20,26,27,40,47,49,52,56,60,61,64,65,72,73,74,77,79,82,83,84,85,90,92,94,97) THEN 26
						*/
						WHEN t1.SC_id_30_38=33 AND ORG_VID = . THEN 28
						WHEN t1.SC_id_30_38=33 AND ORG_VID IN(0,24,28,29,30,31,32,33,34,35,36,39,42,44,58,59,63,67,68,80,81,88,96,98) THEN 4
						WHEN t1.SC_id_30_38=33 AND ORG_VID IN(22,23,25,41,43,45,48,50,51,53,54,55,62,66,69,70,76,99) THEN 13
						WHEN t1.SC_id_30_38=33 AND ORG_VID IN(102,103,105,21,38,46,57,71,75,78,91,93,95) THEN 18
						WHEN t1.SC_id_30_38=33 AND ORG_VID IN(100,101,104,17,18,19,20,26,27,40,47,49,52,56,60,61,64,65,72,73,74,77,79,82,83,84,85,90,92,94,97) THEN 28
						WHEN t1.SC_id_30_38=33 THEN 28
						WHEN t1.SC_id_30_38=34 AND ORG_VID = . THEN 7
						WHEN t1.SC_id_30_38=34 AND ORG_VID IN(100,102,104,17,18,19,37,39,47,49,52,59,61,72,75,78,82,83,86,87,89,90,92,93,94) THEN 23
						WHEN t1.SC_id_30_38=34 AND ORG_VID IN(101,21,22,23,24,26,28,44,46,54,58,63,64,65,66,70,71,73,74,76,77,79,80,91) THEN 14
						WHEN t1.SC_id_30_38=34 AND ORG_VID IN(0,105,25,30,31,32,38,40,43,45,50,53,55,57,62,69,85,95,98,99) THEN 11
						WHEN t1.SC_id_30_38=34 AND ORG_VID IN(103,107,20,27,29,33,34,35,36,41,42,48,51,46,60,67,68,81,84,88,96,97) THEN 7
						WHEN t1.SC_id_30_38=34 THEN 23
						WHEN t1.SC_id_30_38=35 AND ORG_VID = . THEN 32
						WHEN t1.SC_id_30_38=35 AND ORG_VID IN(101,27,30,32,33,34,37,42,43,48,58,65,67,68,87,95,98) THEN 5
						WHEN t1.SC_id_30_38=35 AND ORG_VID IN(102,21,23,25,29,36,46,50,51,53,54,57,59,64,66,70,99) THEN 15
						WHEN t1.SC_id_30_38=35 AND ORG_VID IN(19,39,45,62,71,75,76,80,90,94) THEN 24
						WHEN t1.SC_id_30_38=35 AND ORG_VID IN(0,100,17,18,20,24,26,35,40,44,47,49,52,56,60,61,63,69,72,73,77,78,82,83,84,85,86,88,89,91,92,93,96,97) THEN 32
						WHEN t1.SC_id_30_38=35 AND ORG_VID IN(103,105,22,28,31,38,41,55,74,79,81) THEN 10
						WHEN t1.SC_id_30_38=35 THEN 23
						WHEN t1.SC_id_30_38=36 AND ORG_VID = . THEN 3
						WHEN t1.SC_id_30_38=36 AND ORG_VID IN(25,28,32,42,43,48,49,51,55,56,67,79,95,99) THEN 8
						WHEN t1.SC_id_30_38=36 AND ORG_VID IN(22,23,38,44,45,50,53,54,58,62,66,68,69,70,73,76,77,82) THEN 14
						WHEN t1.SC_id_30_38=36 AND ORG_VID IN(102,105,19,20,21,24,26,40,46,57,59,61,63,64,65,71,72,75,80,85,91,97) THEN 21
						WHEN t1.SC_id_30_38=36 AND ORG_VID IN(0,100,103,107,17,18,35,37,39,47,74,78,83,86,87,88,89,90,92,93,94,96) THEN 30
						WHEN t1.SC_id_30_38=36 AND ORG_VID IN(101,27,29,30,31,33,34,36,41,52,60,81,84,98) THEN 3
						WHEN t1.SC_id_30_38=36 THEN 3
						/*
						WHEN t1.SC_id_30_38=37 AND ORG_VID = . THEN 38
						WHEN t1.SC_id_30_38=37 AND ORG_VID IN(0,102,105,22,23,29,30,31,33,34,35,36,38,40,41,43,49,50,51,55,57,63,68,70,77,80,86,95,99) THEN 5
						WHEN t1.SC_id_30_38=37 AND ORG_VID IN(100,101,48,54) THEN 20
						WHEN t1.SC_id_30_38=37 AND ORG_VID IN(18,21,25,26,28,45,61,62,64,66,74,75,79,91,93) THEN 24
						WHEN t1.SC_id_30_38=37 AND ORG_VID IN(103,107,17,19,20,24,32,37,39,42,44,46,47,52,53,58,59,60,65,8,69,71,72,73,76,78,81,82,84,85,87,88,90,92,94,96,98) THEN 38
						*/
						WHEN t1.SC_id_30_38=37 AND ORG_VID = . THEN 39
						WHEN t1.SC_id_30_38=37 AND ORG_VID IN(0,102,105,22,23,29,30,31,33,34,35,36,38,40,41,43,49,50,51,55,57,63,68,70,77,80,86,95,99) THEN 7
						WHEN t1.SC_id_30_38=37 AND ORG_VID IN(100,101,48,54) THEN 21
						WHEN t1.SC_id_30_38=37 AND ORG_VID IN(103,107,17,19,20,24,32,37,39,42,44,46,47,52,53,58,59,60,65,67,69,71,72,73,76,78,81,82,84,85,87,88,90,92,94,96,98) THEN 39
						WHEN t1.SC_id_30_38=37 AND ORG_VID IN(18,21,25,26,28,45,61,62,64,66,74,75,79,91,93) THEN 26
						WHEN t1.SC_id_30_38=37 THEN 39
						WHEN t1.SC_id_30_38=38 AND ORG_VID = . THEN 27
						WHEN t1.SC_id_30_38=38 AND ORG_VID IN(0,29,30,34,35,36,41,43,48,51,55,60,65,69,7,80,81,85,95,96,99) THEN 6
						WHEN t1.SC_id_30_38=38 AND ORG_VID IN(101,20,31,32,38,40,45,53,54,61,62,64,68,70,75,88,91) THEN 15
						WHEN t1.SC_id_30_38=38 AND ORG_VID IN(100,19,22,25,26,33,49,57,58,66,94) THEN 19
						WHEN t1.SC_id_30_38=38 AND ORG_VID IN(102,103,105,107,17,18,24,27,37,39,42,44,47,56,63,67,72,73,76,78,82,83,84,86,89,90,92,97,98) THEN 27
						WHEN t1.SC_id_30_38=38 AND ORG_VID IN(21,23,28,46,50,57,71,74,79,87,93) THEN 22
						WHEN t1.SC_id_30_38=38 THEN 27
					END) AS wG9_ORG_VID
				, (CASE
						WHEN t1.SC_id_30_38=30 AND QM_new_bki = . THEN 5
						WHEN t1.SC_id_30_38=30 AND QM_new_bki < 122 THEN -1
						WHEN t1.SC_id_30_38=30 AND QM_new_bki >= 122 AND QM_new_bki < 131 THEN 4
						WHEN t1.SC_id_30_38=30 AND QM_new_bki >= 131 AND QM_new_bki < 147 THEN 12
						WHEN t1.SC_id_30_38=30 AND QM_new_bki >= 147 THEN 22
						WHEN t1.SC_id_30_38=31 AND QM_new_bki = . THEN 4
						WHEN t1.SC_id_30_38=31 AND QM_new_bki < 131 THEN -2
						WHEN t1.SC_id_30_38=31 AND QM_new_bki >= 131 AND  QM_new_bki< 145 THEN 9
						WHEN t1.SC_id_30_38=31 AND QM_new_bki >= 145 AND  QM_new_bki< 159 THEN 18
						WHEN t1.SC_id_30_38=31 AND QM_new_bki >= 159 THEN 27
						WHEN t1.SC_id_30_38=32 AND QM_new_bki = . THEN 8
						WHEN t1.SC_id_30_38=32 AND QM_new_bki < 122 THEN -4
						WHEN t1.SC_id_30_38=32 AND QM_new_bki >= 122 AND  QM_new_bki< 134 THEN 4
						WHEN t1.SC_id_30_38=32 AND QM_new_bki >= 134 AND  QM_new_bki< 152 THEN 16
						WHEN t1.SC_id_30_38=32 AND QM_new_bki >= 152 THEN 27
						/*
						WHEN t1.SC_id_30_38=33 AND QM_new_bki = . THEN 15
						WHEN t1.SC_id_30_38=33 AND QM_new_bki < 122 THEN 2
						WHEN t1.SC_id_30_38=33 AND QM_new_bki >= 122 AND QM_new_bki < 134 THEN 9
						WHEN t1.SC_id_30_38=33 AND QM_new_bki >= 134 AND QM_new_bki < 146 THEN 15
						WHEN t1.SC_id_30_38=33 AND QM_new_bki >= 146 AND QM_new_bki < 159 THEN 25
						WHEN t1.SC_id_30_38=33 AND QM_new_bki >= 159 THEN 30
						*/
						WHEN t1.SC_id_30_38=33 AND QM_new_bki = . THEN 16
						WHEN t1.SC_id_30_38=33 AND QM_new_bki < 122 THEN 4
						WHEN t1.SC_id_30_38=33 AND QM_new_bki >= 122 AND QM_new_bki < 134 THEN 11
						WHEN t1.SC_id_30_38=33 AND QM_new_bki >= 134 AND QM_new_bki < 146 THEN 16
						WHEN t1.SC_id_30_38=33 AND QM_new_bki >= 146 AND QM_new_bki < 159 THEN 26
						WHEN t1.SC_id_30_38=33 AND QM_new_bki >= 159 THEN 31
						WHEN t1.SC_id_30_38=34 AND QM_new_bki = . THEN 16
						WHEN t1.SC_id_30_38=34 AND QM_new_bki < 131 THEN 4
						WHEN t1.SC_id_30_38=34 AND QM_new_bki >= 131 AND QM_new_bki < 136 THEN 12
						WHEN t1.SC_id_30_38=34 AND QM_new_bki >= 136 AND QM_new_bki < 152 THEN 16
						WHEN t1.SC_id_30_38=34 AND QM_new_bki >= 152 AND QM_new_bki < 164 THEN 21
						WHEN t1.SC_id_30_38=34 AND QM_new_bki >= 164 THEN 28
						WHEN t1.SC_id_30_38=35 AND QM_new_bki = . THEN 29
						WHEN t1.SC_id_30_38=35 AND QM_new_bki < 127 THEN 1
						WHEN t1.SC_id_30_38=35 AND QM_new_bki >= 127 AND QM_new_bki < 143 THEN 9
						WHEN t1.SC_id_30_38=35 AND QM_new_bki >= 143 AND QM_new_bki < 151 THEN 16
						WHEN t1.SC_id_30_38=35 AND QM_new_bki >= 151 AND QM_new_bki < 163 THEN 22
						WHEN t1.SC_id_30_38=35 AND QM_new_bki >= 163 THEN 29
						WHEN t1.SC_id_30_38=36 AND QM_new_bki = . THEN 23
						WHEN t1.SC_id_30_38=36 AND QM_new_bki < 131 THEN 2
						WHEN t1.SC_id_30_38=36 AND QM_new_bki >= 131 AND QM_new_bki < 143 THEN 9
						WHEN t1.SC_id_30_38=36 AND QM_new_bki >= 143 AND QM_new_bki < 152 THEN 13
						WHEN t1.SC_id_30_38=36 AND QM_new_bki >= 152 AND QM_new_bki < 159 THEN 18
						WHEN t1.SC_id_30_38=36 AND QM_new_bki >= 159 THEN 23
						/*
						WHEN t1.SC_id_30_38=37 AND QM_new_bki = . THEN 32
						WHEN t1.SC_id_30_38=37 AND QM_new_bki < 136 THEN 6
						WHEN t1.SC_id_30_38=37 AND QM_new_bki >= 136 AND QM_new_bki < 146 THEN 13
						WHEN t1.SC_id_30_38=37 AND QM_new_bki >= 146 AND QM_new_bki < 164 THEN 22
						WHEN t1.SC_id_30_38=37 AND QM_new_bki >= 164 AND QM_new_bki < 179 THEN 32
						WHEN t1.SC_id_30_38=37 AND QM_new_bki >= 179 THEN 40
						*/
						WHEN t1.SC_id_30_38=37 AND QM_new_bki = . THEN 35
						WHEN t1.SC_id_30_38=37 AND QM_new_bki < 136 THEN 5
						WHEN t1.SC_id_30_38=37 AND QM_new_bki >= 136 AND QM_new_bki < 146 THEN 13
						WHEN t1.SC_id_30_38=37 AND QM_new_bki >= 146 AND QM_new_bki < 164 THEN 24
						WHEN t1.SC_id_30_38=37 AND QM_new_bki >= 164 AND QM_new_bki < 179 THEN 35
						WHEN t1.SC_id_30_38=37 AND QM_new_bki >= 179 THEN 44
						WHEN t1.SC_id_30_38=38 AND QM_new_bki = . THEN 7
						WHEN t1.SC_id_30_38=38 AND QM_new_bki < 137 THEN 7
						WHEN t1.SC_id_30_38=38 AND QM_new_bki >= 137 AND QM_new_bki < 147 THEN 10
						WHEN t1.SC_id_30_38=38 AND QM_new_bki >= 147 AND QM_new_bki < 152 THEN 16
						WHEN t1.SC_id_30_38=38 AND QM_new_bki >= 152 AND QM_new_bki < 164 THEN 20
						WHEN t1.SC_id_30_38=38 AND QM_new_bki >= 164 THEN 29
					END) AS wG9_QM_new_bki
				, (CASE
						WHEN t1.SC_id_30_38=30 AND SALE_ID_FIRST = . THEN 16
						WHEN t1.SC_id_30_38=30 AND SALE_ID_FIRST = 10 THEN 2
						WHEN t1.SC_id_30_38=30 AND SALE_ID_FIRST IN(13,3) THEN 7
						WHEN t1.SC_id_30_38=30 AND SALE_ID_FIRST IN(1,11,16,17,2,20) THEN 16
						WHEN t1.SC_id_30_38=30 THEN 16
						WHEN t1.SC_id_30_38=31 AND SALE_ID_FIRST = . THEN 17
						WHEN t1.SC_id_30_38=31 AND SALE_ID_FIRST IN(10,17) THEN 4
						WHEN t1.SC_id_30_38=31 AND SALE_ID_FIRST IN(16,3) THEN 14
						WHEN t1.SC_id_30_38=31 AND SALE_ID_FIRST IN(1,11,2) THEN 17
						WHEN t1.SC_id_30_38=31 THEN 17
						WHEN t1.SC_id_30_38=32 AND SALE_ID_FIRST = . THEN 3
						WHEN t1.SC_id_30_38=32 AND SALE_ID_FIRST IN(17,3) THEN 12
						WHEN t1.SC_id_30_38=32 AND SALE_ID_FIRST IN(1,11,13,16,2) THEN 18
						WHEN t1.SC_id_30_38=32 AND SALE_ID_FIRST IN(10,14) THEN 3
						WHEN t1.SC_id_30_38=32 THEN 3
					END) AS wG9_SALE_ID
				, (CASE
						WHEN t1.SC_id_30_38=30 AND SCORE_N_SR_SIMV_veb_bki = . THEN 7
						WHEN t1.SC_id_30_38=30 AND SCORE_N_SR_SIMV_veb_bki < 10.2 THEN 12
						WHEN t1.SC_id_30_38=30 AND SCORE_N_SR_SIMV_veb_bki >= 10.2 AND SCORE_N_SR_SIMV_veb_bki < 11.8 THEN 11
						WHEN t1.SC_id_30_38=30 AND SCORE_N_SR_SIMV_veb_bki >= 11.8 AND SCORE_N_SR_SIMV_veb_bki < 14.5 THEN 8
						WHEN t1.SC_id_30_38=30 AND SCORE_N_SR_SIMV_veb_bki >= 14.5 THEN 5
						WHEN t1.SC_id_30_38=30 THEN 7
						WHEN t1.SC_id_30_38=31 AND SCORE_N_SR_SIMV_veb_bki = . THEN 10
						WHEN t1.SC_id_30_38=31 AND SCORE_N_SR_SIMV_veb_bki < 10.25 THEN 13
						WHEN t1.SC_id_30_38=31 AND SCORE_N_SR_SIMV_veb_bki >= 10.25 AND SCORE_N_SR_SIMV_veb_bki < 11.74 THEN 12
						WHEN t1.SC_id_30_38=31 AND SCORE_N_SR_SIMV_veb_bki >= 11.74 AND SCORE_N_SR_SIMV_veb_bki < 14.5 THEN 10
						WHEN t1.SC_id_30_38=31 AND SCORE_N_SR_SIMV_veb_bki >= 14.5 THEN 9
						WHEN t1.SC_id_30_38=32 AND SCORE_N_SR_SIMV_veb_bki = . THEN 10
						WHEN t1.SC_id_30_38=32 AND SCORE_N_SR_SIMV_veb_bki < 10.3 THEN 16
						WHEN t1.SC_id_30_38=32 AND SCORE_N_SR_SIMV_veb_bki >= 10.3 AND SCORE_N_SR_SIMV_veb_bki < 13.33 THEN 12
						WHEN t1.SC_id_30_38=32 AND SCORE_N_SR_SIMV_veb_bki >= 13.33 THEN 7
					END) AS wG9_SCORE_N_SR_SIMV_bki
				, (CASE
						WHEN t1.SC_id_30_38=30 AND stag_in_months = . THEN 8
						WHEN t1.SC_id_30_38=30 AND stag_in_months < 8 THEN 9
						WHEN t1.SC_id_30_38=30 AND stag_in_months >= 8 AND stag_in_months < 36 THEN 8
						WHEN t1.SC_id_30_38=30 AND stag_in_months >= 36 AND stag_in_months < 48 THEN 9
						WHEN t1.SC_id_30_38=30 AND stag_in_months >= 48 AND stag_in_months < 84 THEN 12
						WHEN t1.SC_id_30_38=30 AND stag_in_months >= 84 THEN 15
						WHEN t1.SC_id_30_38=31 AND stag_in_months = . THEN 8
						WHEN t1.SC_id_30_38=31 AND stag_in_months < 6 THEN 14
						WHEN t1.SC_id_30_38=31 AND stag_in_months >= 6 AND stag_in_months < 60 THEN 8
						WHEN t1.SC_id_30_38=31 AND stag_in_months >= 60 AND stag_in_months < 120 THEN 13
						WHEN t1.SC_id_30_38=31 AND stag_in_months >= 120 THEN 17
						WHEN t1.SC_id_30_38=32 AND stag_in_months = . THEN 7
						WHEN t1.SC_id_30_38=32 AND stag_in_months < 3 THEN 16
						WHEN t1.SC_id_30_38=32 AND stag_in_months >= 3 AND stag_in_months < 24 THEN 7
						WHEN t1.SC_id_30_38=32 AND stag_in_months >= 24 AND stag_in_months < 60 THEN 9
						WHEN t1.SC_id_30_38=32 AND stag_in_months >= 60 AND stag_in_months < 96 THEN 14
						WHEN t1.SC_id_30_38=32 AND stag_in_months >= 96 THEN 19
						/*
						WHEN t1.SC_id_30_38=33 AND stag_in_months = . THEN 20
						WHEN t1.SC_id_30_38=33 AND stag_in_months < 1 THEN 18
						WHEN t1.SC_id_30_38=33 AND stag_in_months >= 1 AND stag_in_months < 24 THEN 12
						WHEN t1.SC_id_30_38=33 AND stag_in_months >= 24 AND stag_in_months < 100 THEN 15
						WHEN t1.SC_id_30_38=33 AND stag_in_months >= 100 THEN 20
						*/
						WHEN t1.SC_id_30_38=33 AND stag_in_months = . THEN 22
						WHEN t1.SC_id_30_38=33 AND stag_in_months < 1 THEN 19
						WHEN t1.SC_id_30_38=33 AND stag_in_months >= 1 AND stag_in_months < 24 THEN 13
						WHEN t1.SC_id_30_38=33 AND stag_in_months >= 24 AND stag_in_months < 100 THEN 17
						WHEN t1.SC_id_30_38=33 AND stag_in_months >= 100 THEN 22
						WHEN t1.SC_id_30_38=35 AND stag_in_months = . THEN 27
						WHEN t1.SC_id_30_38=35 AND stag_in_months < 36 THEN 12
						WHEN t1.SC_id_30_38=35 AND stag_in_months >= 36 AND stag_in_months < 60 THEN 15
						WHEN t1.SC_id_30_38=35 AND stag_in_months >= 60 AND stag_in_months < 84 THEN 19
						WHEN t1.SC_id_30_38=35 AND stag_in_months >= 84 AND stag_in_months < 156 THEN 21
						WHEN t1.SC_id_30_38=35 AND stag_in_months >= 156 THEN 27
						WHEN t1.SC_id_30_38=36 AND stag_in_months = . THEN -7
						WHEN t1.SC_id_30_38=36 AND stag_in_months < 36 THEN 11
						WHEN t1.SC_id_30_38=36 AND stag_in_months >= 36 AND stag_in_months < 60 THEN 13
						WHEN t1.SC_id_30_38=36 AND stag_in_months >= 60 AND stag_in_months < 96 THEN 16
						WHEN t1.SC_id_30_38=36 AND stag_in_months >= 96 THEN 20
					END) AS wG9_stag_in_months
				, (CASE
						WHEN t1.SC_id_30_38=30 AND closed_debt_max_bki = . THEN -1
						WHEN t1.SC_id_30_38=30 AND closed_debt_max_bki <0.1 THEN 10
						WHEN t1.SC_id_30_38=30 AND closed_debt_max_bki >= 0.1 AND closed_debt_max_bki < 0.85 THEN 25
						WHEN t1.SC_id_30_38=30 AND closed_debt_max_bki >= 0.85 AND closed_debt_max_bki < 0.94 THEN 18
						WHEN t1.SC_id_30_38=30 AND closed_debt_max_bki >= 0.94 AND closed_debt_max_bki < 0.99 THEN 9
						WHEN t1.SC_id_30_38=30 AND closed_debt_max_bki >= 0.99 THEN -1
						WHEN t1.SC_id_30_38=31 AND closed_debt_max_bki = . THEN 13
						WHEN t1.SC_id_30_38=31 AND closed_debt_max_bki <0.03 THEN 13
						WHEN t1.SC_id_30_38=31 AND closed_debt_max_bki >= 0.03 AND closed_debt_max_bki < 0.61 THEN 27
						WHEN t1.SC_id_30_38=31 AND closed_debt_max_bki >= 0.61 AND closed_debt_max_bki < 0.89 THEN 24
						WHEN t1.SC_id_30_38=31 AND closed_debt_max_bki >= 0.89 AND closed_debt_max_bki < 0.97 THEN 17
						WHEN t1.SC_id_30_38=31 AND closed_debt_max_bki >= 0.97 AND closed_debt_max_bki < 1.02 THEN 5
						WHEN t1.SC_id_30_38=31 AND closed_debt_max_bki >= 1.02 THEN -2
						WHEN t1.SC_id_30_38=32 AND closed_debt_max_bki = . THEN 15
						WHEN t1.SC_id_30_38=32 AND closed_debt_max_bki <0.27 THEN 15
						WHEN t1.SC_id_30_38=32 AND closed_debt_max_bki >= 0.27 AND closed_debt_max_bki < 0.74 THEN 29
						WHEN t1.SC_id_30_38=32 AND closed_debt_max_bki >= 0.74 AND closed_debt_max_bki < 0.93 THEN 22
						WHEN t1.SC_id_30_38=32 AND closed_debt_max_bki >= 0.93 AND closed_debt_max_bki < 0.98 THEN 12
						WHEN t1.SC_id_30_38=32 AND closed_debt_max_bki >= 0.98 THEN 1
						/*
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki = . THEN 28
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki <0.6 THEN 28
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki >= 0.6 AND closed_debt_max_bki < 0.85 THEN 24
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki >= 0.85 AND closed_debt_max_bki < 0.94 THEN 19
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki >= 0.94 AND closed_debt_max_bki < 0.98 THEN 12
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki >= 0.98 THEN 3
						*/
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki = . THEN 28
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki <0.6 THEN 28
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki >= 0.6 AND closed_debt_max_bki < 0.85 THEN 24
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki >= 0.85 AND closed_debt_max_bki < 0.94 THEN 20
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki >= 0.94 AND closed_debt_max_bki < 0.98 THEN 14
						WHEN t1.SC_id_30_38=33 AND closed_debt_max_bki >= 0.98 THEN 6
						WHEN t1.SC_id_30_38=34 AND closed_debt_max_bki = . THEN 23
						WHEN t1.SC_id_30_38=34 AND closed_debt_max_bki < 0.83 THEN 33
						WHEN t1.SC_id_30_38=34 AND closed_debt_max_bki >= 0.83 AND closed_debt_max_bki < 0.95 THEN 23
						WHEN t1.SC_id_30_38=34 AND closed_debt_max_bki >= 0.95 AND closed_debt_max_bki < 0.99 THEN 14
						WHEN t1.SC_id_30_38=34 AND closed_debt_max_bki >= 0.99 AND closed_debt_max_bki < 1.01 THEN 8
						WHEN t1.SC_id_30_38=34 AND closed_debt_max_bki >= 1.01 THEN 3
						WHEN t1.SC_id_30_38=35 AND closed_debt_max_bki = . THEN 19
						WHEN t1.SC_id_30_38=35 AND closed_debt_max_bki <0.02 THEN 19
						WHEN t1.SC_id_30_38=35 AND closed_debt_max_bki >= 0.02 AND closed_debt_max_bki < 0.85 THEN 25
						WHEN t1.SC_id_30_38=35 AND closed_debt_max_bki >= 0.85 AND closed_debt_max_bki < 0.98 THEN 17
						WHEN t1.SC_id_30_38=35 AND closed_debt_max_bki >= 0.98 THEN 5
						WHEN t1.SC_id_30_38=36 AND closed_debt_max_bki = . THEN 29
						WHEN t1.SC_id_30_38=36 AND closed_debt_max_bki <0.7 THEN 29
						WHEN t1.SC_id_30_38=36 AND closed_debt_max_bki >= 0.7 AND closed_debt_max_bki < 0.88 THEN 25
						WHEN t1.SC_id_30_38=36 AND closed_debt_max_bki >= 0.88 AND closed_debt_max_bki < 0.96 THEN 19
						WHEN t1.SC_id_30_38=36 AND closed_debt_max_bki >= 0.96 AND closed_debt_max_bki < 0.98 THEN 13
						WHEN t1.SC_id_30_38=36 AND closed_debt_max_bki >= 0.98 AND closed_debt_max_bki < 1.01 THEN 8
						WHEN t1.SC_id_30_38=36 AND closed_debt_max_bki >= 1.01 THEN 1
						/*
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki = . THEN 26
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki < 0.01 THEN 26
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki >= 0.01 AND closed_debt_max_bki < 0.59 THEN 30
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki >= 0.59 AND closed_debt_max_bki < 0.93 THEN 23
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki >= 0.93 AND closed_debt_max_bki < 0.98 THEN 11
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki >= 0.98 THEN 6
						*/
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki = . THEN 28
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki < 0.01 THEN 28
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki >= 0.01 AND closed_debt_max_bki < 0.59 THEN 34
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki >= 0.59 AND closed_debt_max_bki < 0.93 THEN 25
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki >= 0.93 AND closed_debt_max_bki < 0.98 THEN 11
						WHEN t1.SC_id_30_38=37 AND closed_debt_max_bki >= 0.98 THEN 5
						WHEN t1.SC_id_30_38=38 AND closed_debt_max_bki = . THEN 9
						WHEN t1.SC_id_30_38=38 AND closed_debt_max_bki < 0.78 THEN 32
						WHEN t1.SC_id_30_38=38 AND closed_debt_max_bki >= 0.78 AND closed_debt_max_bki < 0.93 THEN 26
						WHEN t1.SC_id_30_38=38 AND closed_debt_max_bki >= 0.93 AND closed_debt_max_bki < 0.97 THEN 19
						WHEN t1.SC_id_30_38=38 AND closed_debt_max_bki >= 0.97 AND closed_debt_max_bki < 0.99 THEN 14
						WHEN t1.SC_id_30_38=38 AND closed_debt_max_bki >= 0.99 THEN 9
					END) AS wG9_closed_debt_max_bki
				, (CASE
						WHEN t1.SC_id_30_38=30 AND loan_freq_bki = . THEN 16
						WHEN t1.SC_id_30_38=30 AND loan_freq_bki < 0.18 THEN 9
						WHEN t1.SC_id_30_38=30 AND loan_freq_bki >= 0.18 AND loan_freq_bki < 0.65 THEN 16
						WHEN t1.SC_id_30_38=30 AND loan_freq_bki >= 0.65 AND loan_freq_bki < 1.09 THEN 13
						WHEN t1.SC_id_30_38=30 AND loan_freq_bki >= 1.09 AND loan_freq_bki < 3.43 THEN 8
						WHEN t1.SC_id_30_38=30 AND loan_freq_bki >= 3.43 THEN -2
						WHEN t1.SC_id_30_38=31 AND loan_freq_bki = . THEN 17
						WHEN t1.SC_id_30_38=31 AND loan_freq_bki < 0.16 THEN 12
						WHEN t1.SC_id_30_38=31 AND loan_freq_bki >= 0.16 AND loan_freq_bki < 0.48 THEN 17
						WHEN t1.SC_id_30_38=31 AND loan_freq_bki >= 0.48 AND loan_freq_bki < 0.89 THEN 15
						WHEN t1.SC_id_30_38=31 AND loan_freq_bki >= 0.89 AND loan_freq_bki < 1.64 THEN 11
						WHEN t1.SC_id_30_38=31 AND loan_freq_bki >= 1.64 THEN 3
						WHEN t1.SC_id_30_38=32 AND loan_freq_bki = . THEN 14
						WHEN t1.SC_id_30_38=32 AND loan_freq_bki < 0.19 THEN 14
						WHEN t1.SC_id_30_38=32 AND loan_freq_bki >= 0.19 AND loan_freq_bki < 0.72 THEN 17
						WHEN t1.SC_id_30_38=32 AND loan_freq_bki >= 0.72 AND loan_freq_bki < 1.47 THEN 12
						WHEN t1.SC_id_30_38=32 AND loan_freq_bki >= 1.47 THEN 4
						/*
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki = . THEN 18
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki < 0.26 THEN 20
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki >= 0.26 AND loan_freq_bki < 0.53 THEN 18
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki >= 0.53 AND loan_freq_bki < 1.1 THEN 16
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki >= 1.1 THEN 11
						*/
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki = . THEN 20
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki < 0.26 THEN 22
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki >= 0.26 AND loan_freq_bki < 0.53 THEN 20
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki >= 0.53 AND loan_freq_bki < 1.1 THEN 17
						WHEN t1.SC_id_30_38=33 AND loan_freq_bki >= 1.1 THEN 13
						WHEN t1.SC_id_30_38=34 AND loan_freq_bki = . THEN 14
						WHEN t1.SC_id_30_38=34 AND loan_freq_bki < 0.44 THEN 25
						WHEN t1.SC_id_30_38=34 AND loan_freq_bki >= 0.44 AND loan_freq_bki < 0.94 THEN 19
						WHEN t1.SC_id_30_38=34 AND loan_freq_bki >= 0.94 AND loan_freq_bki < 2.2 THEN 14
						WHEN t1.SC_id_30_38=34 AND loan_freq_bki >= 2.2 AND loan_freq_bki < 3.72 THEN 8
						WHEN t1.SC_id_30_38=34 AND loan_freq_bki >= 3.72 THEN 1
						WHEN t1.SC_id_30_38=35 AND loan_freq_bki = . THEN 19
						WHEN t1.SC_id_30_38=35 AND loan_freq_bki < 0.15 THEN 17
						WHEN t1.SC_id_30_38=35 AND loan_freq_bki >= 0.15 AND loan_freq_bki < 0.43 THEN 19
						WHEN t1.SC_id_30_38=35 AND loan_freq_bki >= 0.43 AND loan_freq_bki < 0.98 THEN 16
						WHEN t1.SC_id_30_38=35 AND loan_freq_bki >= 0.98 THEN 10
						WHEN t1.SC_id_30_38=36 AND loan_freq_bki = . THEN 24
						WHEN t1.SC_id_30_38=36 AND loan_freq_bki < 0.36 THEN 24
						WHEN t1.SC_id_30_38=36 AND loan_freq_bki >= 0.36 AND loan_freq_bki < 0.65 THEN 21
						WHEN t1.SC_id_30_38=36 AND loan_freq_bki >= 0.65 AND loan_freq_bki < 1.29 THEN 15
						WHEN t1.SC_id_30_38=36 AND loan_freq_bki >= 1.29 AND loan_freq_bki < 1.85 THEN 8
						WHEN t1.SC_id_30_38=36 AND loan_freq_bki >= 1.85 THEN 0
						/*
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki = . THEN 24
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki < 0.19 THEN 24
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki >= 0.19 AND loan_freq_bki < 0.46 THEN 21
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki >= 0.46 AND loan_freq_bki < 0.71 THEN 19
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki >= 0.71 THEN 12
						*/
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki = . THEN 25
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki < 0.19 THEN 25
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki >= 0.19 AND loan_freq_bki < 0.46 THEN 22
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki >= 0.46 AND loan_freq_bki < 0.71 THEN 20
						WHEN t1.SC_id_30_38=37 AND loan_freq_bki >= 0.71 THEN 14
						WHEN t1.SC_id_30_38=38 AND loan_freq_bki = . THEN 27
						WHEN t1.SC_id_30_38=38 AND loan_freq_bki < 0.39 THEN 27
						WHEN t1.SC_id_30_38=38 AND loan_freq_bki >= 0.39 AND loan_freq_bki < 0.62 THEN 23
						WHEN t1.SC_id_30_38=38 AND loan_freq_bki >= 0.62 AND loan_freq_bki < 0.84 THEN 20
						WHEN t1.SC_id_30_38=38 AND loan_freq_bki >= 0.84 AND loan_freq_bki < 1.4 THEN 15
						WHEN t1.SC_id_30_38=38 AND loan_freq_bki >= 1.4 THEN 6
					END) AS wG9_loan_freq_bki
				, (CASE
						WHEN t1.SC_id_30_38=30 AND sex1_age = . THEN 9
						WHEN t1.SC_id_30_38=30 AND sex1_age < 27 THEN 8
						WHEN t1.SC_id_30_38=30 AND sex1_age >= 27 AND sex1_age < 48 THEN 9
						WHEN t1.SC_id_30_38=30 AND sex1_age >= 48 AND sex1_age < 60 THEN 10
						WHEN t1.SC_id_30_38=30 AND sex1_age >= 60 THEN 11
						WHEN t1.SC_id_30_38=31 AND sex1_age = . THEN 6
						WHEN t1.SC_id_30_38=31 AND sex1_age < 45 THEN 6
						WHEN t1.SC_id_30_38=31 AND sex1_age >= 45 AND sex1_age < 60 THEN 8
						WHEN t1.SC_id_30_38=31 AND sex1_age >= 60 AND sex1_age < 88 THEN 9
						WHEN t1.SC_id_30_38=31 AND sex1_age >= 88 AND sex1_age < 106 THEN 13
						WHEN t1.SC_id_30_38=31 AND sex1_age >= 106 THEN 18
						WHEN t1.SC_id_30_38=32 AND sex1_age = . THEN 11
						WHEN t1.SC_id_30_38=32 AND sex1_age < 46 THEN 7
						WHEN t1.SC_id_30_38=32 AND sex1_age >= 46 AND sex1_age < 88 THEN 11
						WHEN t1.SC_id_30_38=32 AND sex1_age >= 88 AND sex1_age < 106 THEN 14
						WHEN t1.SC_id_30_38=32 AND sex1_age >= 106 AND sex1_age < 120 THEN 17
						WHEN t1.SC_id_30_38=32 AND sex1_age >= 120 THEN 21
						WHEN t1.SC_id_30_38=35 AND sex1_age = . THEN 16
						WHEN t1.SC_id_30_38=35 AND sex1_age < 31 THEN 9
						WHEN t1.SC_id_30_38=35 AND sex1_age >= 31 AND sex1_age < 38 THEN 13
						WHEN t1.SC_id_30_38=35 AND sex1_age >= 38 AND sex1_age < 70 THEN 16
						WHEN t1.SC_id_30_38=35 AND sex1_age >= 70 AND sex1_age < 82 THEN 21
						WHEN t1.SC_id_30_38=35 AND sex1_age >= 82 THEN 24
						/*
						WHEN t1.SC_id_30_38=37 AND sex1_age = . THEN 26
						WHEN t1.SC_id_30_38=37 AND sex1_age < 57 THEN 7
						WHEN t1.SC_id_30_38=37 AND sex1_age >= 57 AND sex1_age < 100 THEN 12
						WHEN t1.SC_id_30_38=37 AND sex1_age >= 100 AND sex1_age < 108 THEN 20
						WHEN t1.SC_id_30_38=37 AND sex1_age >= 108 AND sex1_age < 120 THEN 21
						WHEN t1.SC_id_30_38=37 AND sex1_age >= 120 THEN 26
						*/
						WHEN t1.SC_id_30_38=37 AND sex1_age = . THEN 28
						WHEN t1.SC_id_30_38=37 AND sex1_age < 57 THEN 9
						WHEN t1.SC_id_30_38=37 AND sex1_age >= 57 AND sex1_age < 100 THEN 13
						WHEN t1.SC_id_30_38=37 AND sex1_age >= 100 AND sex1_age < 108 THEN 21
						WHEN t1.SC_id_30_38=37 AND sex1_age >= 108 AND sex1_age < 120 THEN 22
						WHEN t1.SC_id_30_38=37 AND sex1_age >= 120 THEN 28
					END) AS wG9_sex1_age
				, (CASE
						WHEN t1.SC_id_30_38=31 AND charge3_12 = . THEN 10
						WHEN t1.SC_id_30_38=31 AND charge3_12 < 0.96 THEN 13
						WHEN t1.SC_id_30_38=31 AND charge3_12 >= 0.96 AND charge3_12 < 1.2 THEN 12
						WHEN t1.SC_id_30_38=31 AND charge3_12 >= 1.2 THEN 11
						WHEN t1.SC_id_30_38=32 AND charge3_12 = . THEN 11
						WHEN t1.SC_id_30_38=32 AND charge3_12 < 0.64 THEN 15
						WHEN t1.SC_id_30_38=32 AND charge3_12 >= 0.64 AND charge3_12 < 1.02 THEN 13
						WHEN t1.SC_id_30_38=32 AND charge3_12 >= 1.02 AND charge3_12 < 1.26 THEN 11
						WHEN t1.SC_id_30_38=32 AND charge3_12 >= 1.26 THEN 10
					END) AS wG9_CHARGE_3_12
				, (CASE
						WHEN t1.SC_id_30_38=32 AND ANTIQUITY_veb_bki = . THEN 14
						WHEN t1.SC_id_30_38=32 AND ANTIQUITY_veb_bki < 1 THEN 14
						WHEN t1.SC_id_30_38=32 AND ANTIQUITY_veb_bki >= 1 AND ANTIQUITY_veb_bki < 2 THEN 13
						WHEN t1.SC_id_30_38=32 AND ANTIQUITY_veb_bki >= 2 AND ANTIQUITY_veb_bki < 4 THEN 11
						WHEN t1.SC_id_30_38=32 AND ANTIQUITY_veb_bki >= 4 THEN 9
					END) AS wG9_ANTIQUITY_bki
				, (CASE
						/*
						WHEN t1.SC_id_30_38=33 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY = . THEN 22
						WHEN t1.SC_id_30_38=33 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY < 1 THEN 19
						WHEN t1.SC_id_30_38=33 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY >= 1 AND < 16 THEN 13
						WHEN t1.SC_id_30_38=33 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY >= 16 THEN 6
						*/
						WHEN t1.SC_id_30_38=33 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY = . THEN 23
						WHEN t1.SC_id_30_38=33 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY < 1 THEN 20
						WHEN t1.SC_id_30_38=33 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY >= 1 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY < 16 THEN 14
						WHEN t1.SC_id_30_38=33 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY >= 16 THEN 8
						WHEN t1.SC_id_30_38=34 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY = . THEN 18
						WHEN t1.SC_id_30_38=34 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY < 1 THEN 18
						WHEN t1.SC_id_30_38=34 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY >= 1 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY < 17 THEN 11
						WHEN t1.SC_id_30_38=34 AND DELAY_ALL_DAY_6M_VEB_NO_DELAY >= 17 THEN 9
					END) AS wG9_DELAY_ALL_DAY_6M_VEB
				, (CASE
						/*
						WHEN t1.SC_id_30_38=33 AND OD_sv_ras = . THEN 3
						WHEN t1.SC_id_30_38=33 AND OD_sv_ras < 0.47 THEN 21
						WHEN t1.SC_id_30_38=33 AND OD_sv_ras >= 0.47 AND OD_sv_ras < 0.84 THEN 14
						WHEN t1.SC_id_30_38=33 AND OD_sv_ras >= 0.84 AND OD_sv_ras < 0.95 THEN 10
						WHEN t1.SC_id_30_38=33 AND OD_sv_ras >= 0.95 THEN 3
						*/
						WHEN t1.SC_id_30_38=33 AND OD_SUM_VEB_ALL = . THEN 7
						WHEN t1.SC_id_30_38=33 AND OD_SUM_VEB_ALL < 0.47 THEN 21
						WHEN t1.SC_id_30_38=33 AND OD_SUM_VEB_ALL >= 0.47 AND OD_SUM_VEB_ALL < 0.84 THEN 16
						WHEN t1.SC_id_30_38=33 AND OD_SUM_VEB_ALL >= 0.84 AND OD_SUM_VEB_ALL < 0.95 THEN 13
						WHEN t1.SC_id_30_38=33 AND OD_SUM_VEB_ALL >= 0.95 THEN 7
						WHEN t1.SC_id_30_38=34 AND OD_SUM_VEB_ALL = . THEN 15
						WHEN t1.SC_id_30_38=34 AND OD_SUM_VEB_ALL < 0.4 THEN 22
						WHEN t1.SC_id_30_38=34 AND OD_SUM_VEB_ALL >= 0.4 AND OD_SUM_VEB_ALL < 0.92 THEN 15
						WHEN t1.SC_id_30_38=34 AND OD_SUM_VEB_ALL >= 0.92 AND OD_SUM_VEB_ALL < 0.96 THEN 11
						WHEN t1.SC_id_30_38=34 AND OD_SUM_VEB_ALL >= 0.96 THEN 9
						WHEN t1.SC_id_30_38=36 AND OD_SUM_VEB_ALL = . THEN 20
						WHEN t1.SC_id_30_38=36 AND OD_SUM_VEB_ALL < 0.26 THEN 20
						WHEN t1.SC_id_30_38=36 AND OD_SUM_VEB_ALL >= 0.26 AND OD_SUM_VEB_ALL < 0.38 THEN 17
						WHEN t1.SC_id_30_38=36 AND OD_SUM_VEB_ALL >= 0.38 AND OD_SUM_VEB_ALL < 0.53 THEN 14
						WHEN t1.SC_id_30_38=36 AND OD_SUM_VEB_ALL >= 0.53 AND OD_SUM_VEB_ALL < 0.69 THEN 11
						WHEN t1.SC_id_30_38=36 AND OD_SUM_VEB_ALL >= 0.69 THEN 8
						WHEN t1.SC_id_30_38=38 AND OD_SUM_VEB_ALL = . THEN 27
						WHEN t1.SC_id_30_38=38 AND OD_SUM_VEB_ALL < 0.24 THEN 27
						WHEN t1.SC_id_30_38=38 AND OD_SUM_VEB_ALL >= 0.24 AND OD_SUM_VEB_ALL < 0.38 THEN 22
						WHEN t1.SC_id_30_38=38 AND OD_SUM_VEB_ALL >= 0.38 AND OD_SUM_VEB_ALL < 0.49 THEN 17
						WHEN t1.SC_id_30_38=38 AND OD_SUM_VEB_ALL >= 0.49 AND OD_SUM_VEB_ALL < 0.75 THEN 13
						WHEN t1.SC_id_30_38=38 AND OD_SUM_VEB_ALL >= 0.75 THEN 8
					END) AS wG9_OD_sv_ras
				, (CASE
						/*
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 = . THEN 18
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 < -1.22 THEN 11
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 >= -1.22 AND SPEEDDYNAM_12 < -0.8 THEN 14
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 >= -0.8 AND SPEEDDYNAM_12 < 0 THEN 18
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 >= 0 AND SPEEDDYNAM_12 < 0.35 THEN 19
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 >= 0.35 THEN 12
						*/
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 = . THEN 19
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 < -1.22 THEN 12
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 >= -1.22 AND SPEEDDYNAM_12 < -0.8 THEN 15
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 >= -0.8 AND SPEEDDYNAM_12 < 0 THEN 19
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 >= 0 AND SPEEDDYNAM_12 < 0.35 THEN 21
						WHEN t1.SC_id_30_38=33 AND SPEEDDYNAM_12 >= 0.35 THEN 14
						/*
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 = . THEN 21
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 < -1.26 THEN 14
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 >= -1.26 AND SPEEDDYNAM_12 < -0.94 THEN 24
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 >= -0.94 AND SPEEDDYNAM_12 < 0.31 THEN 21
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 >= 0.31 THEN 16
						*/
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 = . THEN 22
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 < -1.26 THEN 16
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 >= -1.26 AND SPEEDDYNAM_12 < -0.94 THEN 25
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 >= -0.94 AND SPEEDDYNAM_12 < 0.31 THEN 22
						WHEN t1.SC_id_30_38=37 AND SPEEDDYNAM_12 >= 0.31 THEN 18
					END) AS wG9_SPEEDDYNAM_12
				, (CASE
						/*
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI = . THEN 19
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI < 4 THEN 9
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI >= 4 AND TIME_FROM_DELAY_BKI < 7 THEN 12
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI >= 7 AND TIME_FROM_DELAY_BKI < 13 THEN 15
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI >= 13 AND TIME_FROM_DELAY_BKI < 38 THEN 21
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI >= 38 THEN 19
						*/
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI = . THEN 20
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI < 4 THEN 10
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI >= 4 AND TIME_FROM_DELAY_BKI < 7 THEN 13
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI >= 7 AND TIME_FROM_DELAY_BKI < 13 THEN 16
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI >= 13 AND TIME_FROM_DELAY_BKI < 38 THEN 22
						WHEN t1.SC_id_30_38=33 AND TIME_FROM_DELAY_BKI >= 38 THEN 20
						WHEN t1.SC_id_30_38=34 AND TIME_FROM_DELAY_BKI = . THEN 17
						WHEN t1.SC_id_30_38=34 AND TIME_FROM_DELAY_BKI < 4 THEN 10
						WHEN t1.SC_id_30_38=34 AND TIME_FROM_DELAY_BKI >= 4 AND TIME_FROM_DELAY_BKI < 7 THEN 13
						WHEN t1.SC_id_30_38=34 AND TIME_FROM_DELAY_BKI >= 7 AND TIME_FROM_DELAY_BKI < 19 THEN 15
						WHEN t1.SC_id_30_38=34 AND TIME_FROM_DELAY_BKI >= 19 THEN 17
						/*
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI = . THEN 31
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI < 5 THEN 11
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI >= 5 AND < 16 THEN 18
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI >= 16 AND < 32 THEN 26
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI >= 32 THEN 21
						*/
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI = . THEN 32
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI < 5 THEN 13
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI >= 5 AND TIME_FROM_DELAY_BKI < 16 THEN 19
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI >= 16 AND TIME_FROM_DELAY_BKI < 32 THEN 27
						WHEN t1.SC_id_30_38=37 AND TIME_FROM_DELAY_BKI >= 32 THEN 22
					END) AS wG9_TIME_FROM_DELAY_BKI

				, (CASE
						WHEN t1.SC_id_30_38=34 AND CNT_CREDIT_VEB_BKI = . THEN 20
						WHEN t1.SC_id_30_38=34 AND CNT_CREDIT_VEB_BKI < 2 THEN 20
						WHEN t1.SC_id_30_38=34 AND CNT_CREDIT_VEB_BKI >= 2 AND CNT_CREDIT_VEB_BKI < 3 THEN 18
						WHEN t1.SC_id_30_38=34 AND CNT_CREDIT_VEB_BKI >= 3 AND CNT_CREDIT_VEB_BKI < 5 THEN 15
						WHEN t1.SC_id_30_38=34 AND CNT_CREDIT_VEB_BKI >= 5 AND CNT_CREDIT_VEB_BKI < 7 THEN 11
						WHEN t1.SC_id_30_38=34 AND CNT_CREDIT_VEB_BKI >= 7 THEN 7
					END) AS wG9_CNT_CREDIT_VEB_BKI
				, (CASE
						WHEN t1.SC_id_30_38=34 AND sd_20_28 = . THEN 20
						WHEN t1.SC_id_30_38=34 AND sd_20_28 < 0.02 THEN 20
						WHEN t1.SC_id_30_38=34 AND sd_20_28 >= 0.02 AND sd_20_28 < 0.04 THEN 16
						WHEN t1.SC_id_30_38=34 AND sd_20_28 >= 0.04 AND sd_20_28 < 0.07 THEN 11
						WHEN t1.SC_id_30_38=34 AND sd_20_28 >= 0.07 THEN 8
						WHEN t1.SC_id_30_38=35 AND sd_20_28 = . THEN 19
						WHEN t1.SC_id_30_38=35 AND sd_20_28 < 0.02 THEN 19
						WHEN t1.SC_id_30_38=35 AND sd_20_28 >= 0.02 AND sd_20_28 < 0.04 THEN 18
						WHEN t1.SC_id_30_38=35 AND sd_20_28 >= 0.04 AND sd_20_28 < 0.06 THEN 16
						WHEN t1.SC_id_30_38=35 AND sd_20_28 >= 0.06 THEN 14
						WHEN t1.SC_id_30_38=36 AND sd_20_28 = . THEN 20
						WHEN t1.SC_id_30_38=36 AND sd_20_28 < 0.02 THEN 20
						WHEN t1.SC_id_30_38=36 AND sd_20_28 >= 0.02 AND sd_20_28 < 0.04 THEN 18
						WHEN t1.SC_id_30_38=36 AND sd_20_28 >= 0.04 AND sd_20_28 < 0.06 THEN 13
						WHEN t1.SC_id_30_38=36 AND sd_20_28 >= 0.06 AND sd_20_28 < 0.09 THEN 9
						WHEN t1.SC_id_30_38=36 AND sd_20_28 >= 0.09 THEN 5
						/*
						WHEN t1.SC_id_30_38=37 AND SD = . THEN 23
						WHEN t1.SC_id_30_38=37 AND SD < 0.01 THEN 23
						WHEN t1.SC_id_30_38=37 AND SD >= 0.01 AND < 0.02 THEN 21
						WHEN t1.SC_id_30_38=37 AND SD >= 0.02 AND < 0.03 THEN 18
						WHEN t1.SC_id_30_38=37 AND SD >= 0.03 THEN 14
						*/
						WHEN t1.SC_id_30_38=37 AND sd_20_28 = . THEN 25
						WHEN t1.SC_id_30_38=37 AND sd_20_28 < 0.01 THEN 25
						WHEN t1.SC_id_30_38=37 AND sd_20_28 >= 0.01 AND sd_20_28 < 0.02 THEN 23
						WHEN t1.SC_id_30_38=37 AND sd_20_28 >= 0.02 AND sd_20_28 < 0.03 THEN 19
						WHEN t1.SC_id_30_38=37 AND sd_20_28 >= 0.03 THEN 15
						WHEN t1.SC_id_30_38=38 AND sd_20_28 = . THEN 21
						WHEN t1.SC_id_30_38=38 AND sd_20_28 < 0.01 THEN 21
						WHEN t1.SC_id_30_38=38 AND sd_20_28 >= 0.01 AND sd_20_28 < 0.02 THEN 19
						WHEN t1.SC_id_30_38=38 AND sd_20_28 >= 0.02 AND sd_20_28 < 0.04 THEN 15
						WHEN t1.SC_id_30_38=38 AND sd_20_28 >= 0.04 THEN 10
					END) AS wG9_SD
				, (CASE
						WHEN t1.SC_id_30_38=34 AND regions_id = . THEN 2
						WHEN t1.SC_id_30_38=34 AND regions_id IN(13,21,22,26,30,34,37,4,42,54,56,67,71,72,73,76) THEN 2
						WHEN t1.SC_id_30_38=34 AND regions_id IN(16,18,23,45,48,55,57,58,62,63,64,69,74,77,89) THEN 8
						WHEN t1.SC_id_30_38=34 AND regions_id IN(10,19,2,24,32,35,38,39,40,43,44,46,50,52,61,66,68,70,75,86) THEN 15
						WHEN t1.SC_id_30_38=34 AND regions_id IN(11,12,14,17,25,27,28,29,3,31,33,36,41,47,49,51,53,59,60,65,78,79) THEN 21
						WHEN t1.SC_id_30_38=34 THEN 21
						WHEN t1.SC_id_30_38=36 AND regions_id = . THEN 4
						WHEN t1.SC_id_30_38=36 AND regions_id IN(16,18,2,22,23,26,30,31,32,34,36,37,42,43,45,46,52,54,55,56,57,58,61,62,63,64,66,67,68,69,72,73,74,76,77,86) THEN 4
						WHEN t1.SC_id_30_38=36 AND regions_id IN(10,19,33,35,39,40,48,51,53,59,60,70) THEN 12
						WHEN t1.SC_id_30_38=36 AND regions_id IN(11,24,29,3,38,47,50) THEN 15
						WHEN t1.SC_id_30_38=36 AND regions_id IN(13,14,17,20,21,25,27,28,41,49,65,71,75,78,79,89) THEN 18
						WHEN t1.SC_id_30_38=36 THEN 4
						/*
						WHEN t1.SC_id_30_38=37 AND regions_id = . THEN 25
						WHEN t1.SC_id_30_38=37 AND regions_id IN(14,18,22,26,30,31,34,37,39,40,42,45,46,48,51,52,55,56,58,61,64,66,68,70) THEN 7
						WHEN t1.SC_id_30_38=37 AND regions_id IN(24,25,35,47,75) THEN 20
						WHEN t1.SC_id_30_38=37 AND regions_id IN(11,12,13,16,19,2,21,23,27,29,32,38,41,43,44,50,54,57,59,62,63,65,67,69,74,79,80,86,89) THEN 25
						WHEN t1.SC_id_30_38=37 AND regions_id IN(10,28,3,33,36,53,60) THEN 18
						*/
						WHEN t1.SC_id_30_38=37 AND regions_id = . THEN 27
						WHEN t1.SC_id_30_38=37 AND regions_id IN(14,18,22,26,30,31,34,37,39,40,42,45,46,48,51,52,55,56,58,61,64,66,68,70,71,72,73,76,78) THEN 8
						WHEN t1.SC_id_30_38=37 AND regions_id IN(10,28,3,33,36,53,60) THEN 20
						WHEN t1.SC_id_30_38=37 AND regions_id IN(24,25,35,47,75) THEN 22
						WHEN t1.SC_id_30_38=37 AND regions_id IN(11,12,13,16,19,2,21,23,27,29,32,38,41,43,44,50,54,57,59,62,63,65,67,69,74,79,80,86,89) THEN 27
						WHEN t1.SC_id_30_38=37 THEN 27
						WHEN t1.SC_id_30_38=38 AND regions_id = . THEN 25
						WHEN t1.SC_id_30_38=38 AND regions_id IN(12,13,14,17,21,25,27,4,41,44,77,79,85,89) THEN 25
						WHEN t1.SC_id_30_38=38 AND regions_id IN(11,24,3,30,38,53,78) THEN 20
						WHEN t1.SC_id_30_38=38 AND regions_id IN(19,29,31,32,35,36,39,40,47,50,51,59,60,68) THEN 14
						WHEN t1.SC_id_30_38=38 AND regions_id IN(10,16,18,2,22,23,26,33,34,37,42,43,45,46,48,52,55,56,57,58,61,62,63,64,66,67,69,70,71,72,73,74,76,86) THEN 3
						WHEN t1.SC_id_30_38=38 AND regions_id IN(28,65,75) THEN 22
						WHEN t1.SC_id_30_38=38 THEN 25
					END) AS wG9_region_simb1
				, (CASE
						WHEN t1.SC_id_30_38=35 AND DELAY_1_89_NUM_BKI = . THEN 15
						WHEN t1.SC_id_30_38=35 AND DELAY_1_89_NUM_BKI < 1 THEN 21
						WHEN t1.SC_id_30_38=35 AND DELAY_1_89_NUM_BKI >= 1 AND DELAY_1_89_NUM_BKI< 3 THEN 18
						WHEN t1.SC_id_30_38=35 AND DELAY_1_89_NUM_BKI >= 3 THEN 15
					END) AS wG9_DELAY_1_89_NUM_BKI
				, (CASE
						WHEN t1.SC_id_30_38=35 AND MAX_12_rate = . THEN 18
						WHEN t1.SC_id_30_38=35 AND MAX_12_rate < 10 THEN 16
						WHEN t1.SC_id_30_38=35 AND MAX_12_rate >= 10 AND MAX_12_rate < 20 THEN 18
						WHEN t1.SC_id_30_38=35 AND MAX_12_rate >= 20 AND MAX_12_rate < 30 THEN 17
						WHEN t1.SC_id_30_38=35 AND MAX_12_rate >= 30 THEN 15
					END) AS wG9_MAX_12_rate
				, (CASE
						WHEN t1.SC_id_30_38=35 AND SCORE_M_SR_SIMV_veb_bki = . THEN 13
						WHEN t1.SC_id_30_38=35 AND SCORE_M_SR_SIMV_veb_bki < 10.78 THEN 20
						WHEN t1.SC_id_30_38=35 AND SCORE_M_SR_SIMV_veb_bki >= 10.78 AND SCORE_M_SR_SIMV_veb_bki < 13.78 THEN 16
						WHEN t1.SC_id_30_38=35 AND SCORE_M_SR_SIMV_veb_bki >= 13.78 AND SCORE_M_SR_SIMV_veb_bki < 17.89 THEN 13
						WHEN t1.SC_id_30_38=35 AND SCORE_M_SR_SIMV_veb_bki >= 17.89 THEN 8
					END) AS wG9_SCORE_M_SR_SIMV_bki
				, (CASE
						WHEN t1.SC_id_30_38=35 AND SCORE_N_SR_SIMV_veb = . THEN 10
						WHEN t1.SC_id_30_38=35 AND SCORE_N_SR_SIMV_veb < 11.25 THEN 26
						WHEN t1.SC_id_30_38=35 AND SCORE_N_SR_SIMV_veb >= 11.25 AND SCORE_N_SR_SIMV_veb < 13.64 THEN 17
						WHEN t1.SC_id_30_38=35 AND SCORE_N_SR_SIMV_veb >= 13.64 AND SCORE_N_SR_SIMV_veb < 20.33 THEN 10
						WHEN t1.SC_id_30_38=35 AND SCORE_N_SR_SIMV_veb >= 20.33 THEN 0
						WHEN t1.SC_id_30_38=36 AND SCORE_N_SR_SIMV_veb = . THEN 1
						WHEN t1.SC_id_30_38=36 AND SCORE_N_SR_SIMV_veb < 10.32 THEN 19
						WHEN t1.SC_id_30_38=36 AND SCORE_N_SR_SIMV_veb >= 10.32 AND SCORE_N_SR_SIMV_veb < 11.33 THEN 14
						WHEN t1.SC_id_30_38=36 AND SCORE_N_SR_SIMV_veb >= 11.33 AND SCORE_N_SR_SIMV_veb < 14.94 THEN 12
						WHEN t1.SC_id_30_38=36 AND SCORE_N_SR_SIMV_veb >= 14.94 THEN 8
						WHEN t1.SC_id_30_38=38 AND SCORE_N_SR_SIMV_veb = . THEN 21
						WHEN t1.SC_id_30_38=38 AND SCORE_N_SR_SIMV_veb < 10.37 THEN 21
						WHEN t1.SC_id_30_38=38 AND SCORE_N_SR_SIMV_veb >= 10.37 AND SCORE_N_SR_SIMV_veb < 10.95 THEN 18
						WHEN t1.SC_id_30_38=38 AND SCORE_N_SR_SIMV_veb >= 10.95 AND SCORE_N_SR_SIMV_veb < 11.5 THEN 16
						WHEN t1.SC_id_30_38=38 AND SCORE_N_SR_SIMV_veb >= 11.5 AND SCORE_N_SR_SIMV_veb < 14.42 THEN 15
						WHEN t1.SC_id_30_38=38 AND SCORE_N_SR_SIMV_veb >= 14.42 THEN 13
					END) AS wG9_SCORE_N_SR_SIMV_veb
				, (CASE
						WHEN t1.SC_id_30_38=36 AND DYNAMICS_QUALITY_bki = . THEN 3
						WHEN t1.SC_id_30_38=36 AND DYNAMICS_QUALITY_bki < 1 THEN 14
						WHEN t1.SC_id_30_38=36 AND DYNAMICS_QUALITY_bki >= 1 AND DYNAMICS_QUALITY_bki < 1.01 THEN 19
						WHEN t1.SC_id_30_38=36 AND DYNAMICS_QUALITY_bki >= 1.01 AND DYNAMICS_QUALITY_bki < 1.05 THEN 13
						WHEN t1.SC_id_30_38=36 AND DYNAMICS_QUALITY_bki >= 1.05 AND DYNAMICS_QUALITY_bki < 1.13 THEN 12
						WHEN t1.SC_id_30_38=36 AND DYNAMICS_QUALITY_bki >= 1.13 THEN 10
					END) AS wG9_DYNAMICS_QUALITY_bki
				, (CASE
						WHEN t1.SC_id_30_38=36 AND MONTHSWENTWHENLASTDELIQ IN(.,0) THEN 17
						WHEN t1.SC_id_30_38=36 AND MONTHSWENTWHENLASTDELIQ < 4 THEN 11
						WHEN t1.SC_id_30_38=36 AND MONTHSWENTWHENLASTDELIQ >= 4 AND MONTHSWENTWHENLASTDELIQ < 8 THEN 14
						WHEN t1.SC_id_30_38=36 AND MONTHSWENTWHENLASTDELIQ >= 8 AND MONTHSWENTWHENLASTDELIQ < 14 THEN 15
						WHEN t1.SC_id_30_38=36 AND MONTHSWENTWHENLASTDELIQ >= 14 THEN 17
						WHEN t1.SC_id_30_38=38 AND MONTHSWENTWHENLASTDELIQ IN(.,0) THEN 24
						WHEN t1.SC_id_30_38=38 AND MONTHSWENTWHENLASTDELIQ < 4 THEN 12
						WHEN t1.SC_id_30_38=38 AND MONTHSWENTWHENLASTDELIQ >= 4 AND MONTHSWENTWHENLASTDELIQ < 7 THEN 15
						WHEN t1.SC_id_30_38=38 AND MONTHSWENTWHENLASTDELIQ >= 7 AND MONTHSWENTWHENLASTDELIQ < 11 THEN 18
						WHEN t1.SC_id_30_38=38 AND MONTHSWENTWHENLASTDELIQ >= 11 THEN 20
					END) AS wG9_MONTHSWENTWHENLASTDELIQ1
				, (CASE
						WHEN t1.SC_id_30_38=36 AND SUMMA_DEBT_BKI_RATE_MATCHER = . THEN 17
						WHEN t1.SC_id_30_38=36 AND SUMMA_DEBT_BKI_RATE_MATCHER < 21515 THEN 17
						WHEN t1.SC_id_30_38=36 AND SUMMA_DEBT_BKI_RATE_MATCHER >= 21515 AND SUMMA_DEBT_BKI_RATE_MATCHER < 78126 THEN 15
						WHEN t1.SC_id_30_38=36 AND SUMMA_DEBT_BKI_RATE_MATCHER >= 78126 AND SUMMA_DEBT_BKI_RATE_MATCHER < 201724 THEN 13
						WHEN t1.SC_id_30_38=36 AND SUMMA_DEBT_BKI_RATE_MATCHER >= 201724 AND SUMMA_DEBT_BKI_RATE_MATCHER < 352705 THEN 12
						WHEN t1.SC_id_30_38=36 AND SUMMA_DEBT_BKI_RATE_MATCHER >= 352705 THEN 11
					END) AS wG9_SUMMA_DEBT_BKI_RATE_MATCHER
				, (CASE
						WHEN t1.SC_id_30_38=36 AND summ_aggregate = . THEN 17
						WHEN t1.SC_id_30_38=36 AND summ_aggregate < 146585.82 THEN 21
						WHEN t1.SC_id_30_38=36 AND summ_aggregate >= 146585.82 AND summ_aggregate < 221068.34 THEN 17
						WHEN t1.SC_id_30_38=36 AND summ_aggregate >= 221068.34 AND summ_aggregate < 318421 THEN 15
						WHEN t1.SC_id_30_38=36 AND summ_aggregate >= 318421 AND summ_aggregate < 523232.76 THEN 12
						WHEN t1.SC_id_30_38=36 AND summ_aggregate >= 523232.76 THEN 8
					END) AS wG9_sovokup
				, (CASE
						/*
						WHEN t1.SC_id_30_38=37 AND DEBT_all = . THEN 13
						WHEN t1.SC_id_30_38=37 AND DEBT_all < 5000 THEN 13
						WHEN t1.SC_id_30_38=37 AND DEBT_all >= 5000 AND DEBT_all < 20000 THEN 9
						WHEN t1.SC_id_30_38=37 AND DEBT_all >= 20000 AND DEBT_all < 49000 THEN 18
						WHEN t1.SC_id_30_38=37 AND DEBT_all >= 49000 >= AND DEBT_all < 104000 THEN 24
						WHEN t1.SC_id_30_38=37 AND DEBT_all >= 104000 THEN 29
						*/
						WHEN t1.SC_id_30_38=37 AND DEBT_all = . THEN 19
						WHEN t1.SC_id_30_38=37 AND DEBT_all < 20000 THEN 19
						WHEN t1.SC_id_30_38=37 AND DEBT_all >= 20000 AND DEBT_all < 49000 THEN 21
						WHEN t1.SC_id_30_38=37 AND DEBT_all >= 49000 AND DEBT_all < 104000 THEN 22
						WHEN t1.SC_id_30_38=37 AND DEBT_all >= 104000 THEN 24
						WHEN t1.SC_id_30_38=38 AND DEBT_all = . THEN 22
						WHEN t1.SC_id_30_38=38 AND DEBT_all < 88000 THEN 27
						WHEN t1.SC_id_30_38=38 AND DEBT_all >= 88000 AND DEBT_all < 251000 THEN 22
						WHEN t1.SC_id_30_38=38 AND DEBT_all >= 251000 AND DEBT_all < 430000 THEN 16
						WHEN t1.SC_id_30_38=38 AND DEBT_all >= 430000 AND DEBT_all < 635000 THEN 11
						WHEN t1.SC_id_30_38=38 AND DEBT_all >= 104000 THEN 7
					END) AS wG9_DEBT_all
				, (CASE
						/*
						WHEN t1.SC_id_30_38=37 AND DEFOLT_veb_bki = . THEN 22
						WHEN t1.SC_id_30_38=37 AND DEFOLT_veb_bki < 1 THEN 22
						WHEN t1.SC_id_30_38=37 AND DEFOLT_veb_bki >= 1 THEN 8
						*/
						WHEN t1.SC_id_30_38=37 AND DEFOLT_veb_bki = . THEN 23
						WHEN t1.SC_id_30_38=37 AND DEFOLT_veb_bki < 1 THEN 23
						WHEN t1.SC_id_30_38=37 AND DEFOLT_veb_bki >= 1 THEN 11
					END) AS wG9_DEFOLT_bki
				, (CASE
						/*
						WHEN t1.SC_id_30_38=37 AND DYNAMICS_QUALITY_veb = . THEN 24
						WHEN t1.SC_id_30_38=37 AND DYNAMICS_QUALITY_veb < 0.86 THEN 12
						WHEN t1.SC_id_30_38=37 AND DYNAMICS_QUALITY_veb >= 0.86 AND DYNAMICS_QUALITY_veb < 1.08 THEN 24
						WHEN t1.SC_id_30_38=37 AND DYNAMICS_QUALITY_veb >= 1.8 THEN 11
						*/
						WHEN t1.SC_id_30_38=37 AND DYNAMICS_QUALITY_veb = . THEN 26
						WHEN t1.SC_id_30_38=37 AND DYNAMICS_QUALITY_veb < 0.86 THEN 14
						WHEN t1.SC_id_30_38=37 AND DYNAMICS_QUALITY_veb >= 0.86 AND DYNAMICS_QUALITY_veb < 1.08 THEN 26
						WHEN t1.SC_id_30_38=37 AND DYNAMICS_QUALITY_veb >= 1.08 THEN 12
					END) AS wG9_DYNAMICS_QUALITY_veb
				, (CASE
						WHEN t1.SC_id_30_38=38 AND CNT_CREDIT_VEB = . THEN 16
						WHEN t1.SC_id_30_38=38 AND CNT_CREDIT_VEB < 1 THEN 15
						WHEN t1.SC_id_30_38=38 AND CNT_CREDIT_VEB >= 1 AND CNT_CREDIT_VEB < 2 THEN 16
						WHEN t1.SC_id_30_38=38 AND CNT_CREDIT_VEB >= 2 AND CNT_CREDIT_VEB < 3 THEN 18
						WHEN t1.SC_id_30_38=38 AND CNT_CREDIT_VEB >= 3 THEN 19
					END) AS wG9_CNT_CREDIT_VEB
				, (CASE
						WHEN t1.SC_id_30_38=38 AND PERIOD_decl = . THEN 28
						WHEN t1.SC_id_30_38=38 AND PERIOD_decl < 24 THEN 28
						WHEN t1.SC_id_30_38=38 AND PERIOD_decl >= 24 AND PERIOD_decl < 60 THEN 19
						WHEN t1.SC_id_30_38=38 AND PERIOD_decl >= 60 THEN 14
					END) AS wG9_PERIOD
			FROM trakhach._rez_tmp_period AS t1
		) AS rezalt;
DROP TABLE trakhach._rez_tmp_period;
QUIT;
