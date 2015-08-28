proc sql;
CREATE TABLE trakhach._REZ_201210_201407_R_NEW (COMPRESS = BINARY) AS 
 SELECT rezalt.*, 
	/* перерасчитанный r_mark. Изменено только значение w_model_quality_veb_bki как w_model_quality_veb_bki_new */
	(CASE 
		WHEN rezalt.SC_id_new=6
		THEN rezalt.w_education_new
		+ rezalt.w_household_new
		+ rezalt.w_org_vid_new
		+ rezalt.w_model_quality_veb_bki_n
		+ rezalt.w_reg_groupe_new
		+ rezalt.w_SD_new
		+ rezalt.w_sex_age_new
		+ rezalt.w_specif_new
		+ rezalt.w_stag_new
		+ rezalt.w_autofilters_new
		+ rezalt.w_channel_groups
		WHEN rezalt.SC_id_new=7
		THEN rezalt.w_d_new
		+ rezalt.w_education_new
		+ rezalt.w_household_new
		+ rezalt.w_org_vid_new
		+ rezalt.w_model_quality_veb_bki_n
		+ rezalt.w_sex_age_new
		+ rezalt.w_stag_new
		+ rezalt.w_autofilters_new
		+ rezalt.w_bnk_type 
		+ rezalt.w_channel_groups 
		+ rezalt.w_specif_new
		WHEN rezalt.SC_id_new=8 
		THEN rezalt.w_d_new
		+ rezalt.w_education_new 
		+ rezalt.w_household_new
		+ rezalt.w_org_vid_new /*90.79*/
		+ rezalt.w_model_quality_veb_bki_n
		+ rezalt.w_sex_age_new
		+ rezalt.w_specif_new /*94.16*/
		+ rezalt.w_stag_new
		+ rezalt.w_autofilters_new
		+ rezalt.w_bnk_type 
		+ rezalt.w_channel_groups
		WHEN rezalt.SC_id_new=12 /*81.88*/
		THEN rezalt.w_age_new
		+ rezalt.w_education_new 
		+ rezalt.w_cnt_credit_veb 
		+ rezalt.w_od_sum_veb
		+ rezalt.w_org_vid_new /*94.09*/
		+ rezalt.w_model_quality_veb_bki_n
		+ rezalt.w_reg_groupe_new 
		+ rezalt.w_SD_new
		+ rezalt.w_specif_new /*91.47*/
		+ rezalt.w_stag_new
		+ rezalt.w_antiquity_veb_new
		+ rezalt.w_autofilters_new
		+ rezalt.w_channel_groups
		WHEN rezalt.SC_id_new=13 /*84.54*/
		THEN rezalt.w_age_new 
		+ rezalt.w_education_new
		+ rezalt.w_household_new
		+ rezalt.w_k_od
		+ rezalt.w_org_vid_new
		+ rezalt.w_model_quality_veb_bki_n
		+ rezalt.w_reg_grp_channel
		+ rezalt.w_sd_new
		+ rezalt.w_specif_new
		+ rezalt.w_srok_sovocup /*93.08 +12, -5, +17, -12*/
		+ rezalt.w_stag_new
		+ rezalt.w_antiquity_veb_new
		+ rezalt.w_autofilters_new
		+ rezalt.w_l_ki_veb_bki_new
		WHEN rezalt.SC_id_new=14 /*68.09*/
		THEN rezalt.w_education_new 
		+ rezalt.w_household_new
		+ rezalt.w_cnt_credit_veb 
		+ rezalt.w_od_sum_veb 
		+ rezalt.w_org_vid_new /*79.34*/
		+ rezalt.w_model_quality_veb_bki_n
		+ rezalt.w_reg_groupe_new 
		+ rezalt.w_SD_new
		+ rezalt.w_sex_age_new
		+ rezalt.w_specif_new /*77.93*/
		+ rezalt.w_stag_new 
		+ rezalt.w_autofilters_new
		+ rezalt.w_channel_groups /*88.19*/
		WHEN rezalt.SC_id_new=15 /*77.17*/
		THEN rezalt.w_education_new
		+ rezalt.w_household_new
		+ rezalt.w_cnt_credit_veb
		+ rezalt.w_org_vid_new
		+ rezalt.w_model_quality_veb_bki_n
		+ rezalt.w_reg_grp_channel
		+ rezalt.w_sd_new
		+ rezalt.w_sex_age_new
		+ rezalt.w_specif_new /*91.85*/
		+ rezalt.w_srok_sovocup /*90.19 +16, -8, +8, -16*/
		+ rezalt.w_stag_new
		+ rezalt.w_autofilters_new
		+ rezalt.w_dd1chs
		+ rezalt.w_l_ki_veb_new
		WHEN rezalt.SC_id_new=16 /*73.19*/
		THEN rezalt.w_cities_type 
		+ rezalt.w_d_new
		+ rezalt.w_education_new 
		+ rezalt.w_cnt_credit_veb 
		+ rezalt.w_od_sum_veb
		+ rezalt.w_org_vid_new /*84.09*/
		+ rezalt.w_model_quality_veb_bki_n
		+ rezalt.w_reg_groupe_new 
		+ rezalt.w_sex1_age 
		+ rezalt.w_specif_new /*82.27*/
		+ rezalt.w_stag_new /*86.60*/
		+ rezalt.w_autofilters_new
		+ rezalt.w_l_ki_veb_new
		WHEN rezalt.SC_id_new=17 /*79.87*/
		THEN rezalt.w_d_new
		+rezalt.w_education_new
		+rezalt.w_household_new
		+rezalt.w_k_od
		+rezalt.w_org_vid_new /*92.14*/
		+rezalt.w_model_quality_veb_bki_n
		+rezalt.w_reg_grp_channel
		+rezalt.w_sex_age_new
		+rezalt.w_specif_new /*91.70*/
		+rezalt.w_srok_sovocup /*92.31*/
		+rezalt.w_stag_new /*95.19*/
		+rezalt.w_autofilters_new
		+rezalt.w_dd1chs
		+rezalt.w_l_ki_veb_new
		END) AS r_mark_new
	/*РАСЧЕТ R_MARK_20_28 для карт 20-28 на авг. 2014*/
	,(CASE 
		WHEN rezalt.SC_id_20_28=20
		THEN rezalt.w2_bnk_type
			+ rezalt.w2_format_groups
			+ rezalt.w2_org_vid
			+ rezalt.w2_d
			+ rezalt.w2_dynamics_quality_bki
			+ rezalt.w2_education
			+ rezalt.w2_household
			+ rezalt.w2_max_12_rate
			+ ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki)
			+ rezalt.w2_score_n_sr_simv_veb_bki
			+ rezalt.w2_sex_age_derivative
			+ rezalt.w2_stag_in_months
			+ rezalt.w2_type_client
			+ rezalt.w2_charge3_12
		WHEN rezalt.SC_id_20_28=21
		THEN 
			rezalt.w2_format_groups /*ок*/
			+ rezalt.w2_org_vid /*ок*/
			+ rezalt.w2_charge3_12 /*ок*/
			+ rezalt.w2_d /*ок*/
			+ rezalt.w2_education /*ок*/
			+ rezalt.w2_max_12_rate /*ок*/
			+ rezalt.w2_pensioner /*ок*/
			+ ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki) /*ок*/
			+ rezalt.w2_score_n_sr_simv_veb_bki /*ок*/
			+ rezalt.w2_sex_age_derivative /*ок*/
			+ rezalt.w2_stag_in_months /*ок*/
			+ rezalt.w2_type_client /*ок*/
			+ rezalt.w2_monthswentwhenlastdeliq /*ок*/
		WHEN rezalt.SC_id_20_28=22 
		THEN 
			rezalt.w2_format_groups
			+ rezalt.w2_org_vid 
			+ rezalt.w2_charge3_12
			+ rezalt.w2_d
			+ rezalt.w2_household
			+ rezalt.w2_max_12_rate
			+ rezalt.w2_monthswentwhenlastdeliq
			+ ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki)
			+ rezalt.w2_sex_age_derivative
			+ rezalt.w2_stag_in_months
			+ rezalt.w2_score_n_sr_simv_veb_bki
			+ rezalt.w2_antiquity_veb_bki
			+ rezalt.w2_type_client
		WHEN rezalt.SC_id_20_28=23
		THEN ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki)
			+ rezalt.w2_d
			+ rezalt.w2_dynamics_charge_bki
			+ rezalt.w2_od_sum_veb_all
			+ rezalt.w2_aver_6_rate
			+ rezalt.w2_reliability_bki
			+ rezalt.w2_score_m_sr_simv_veb
			+ rezalt.w2_speeddynam_12
			+ rezalt.w2_monthswentwhenlastdeliq
			+ rezalt.w2_prodoljitelnost_ki_veb
			+ rezalt.w2_sex_age_derivative
			+ rezalt.w2_sko_12
		WHEN rezalt.SC_id_20_28=24
		THEN ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki)
			+ rezalt.w2_dynamics_charge_bki
			+ rezalt.w2_education
			+ rezalt.w2_od_sum_veb_all
			+ rezalt.w2_org_vid
			+ rezalt.w2_cnt_credit_veb
			+ rezalt.w2_format_groups
			+ rezalt.w2_sd
			+ rezalt.w2_stag_in_months
			+ rezalt.w2_sum_6_rate
			+ rezalt.w2_prodoljitelnost_ki_veb
			+ rezalt.w2_scorecard_region_group_v2p
			+ rezalt.w2_sex_age_derivative
		WHEN rezalt.SC_id_20_28=25 
		THEN ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki)
			+ rezalt.w2_education
			+ rezalt.w2_household
			+ rezalt.w2_max_12_rate
			+ rezalt.w2_org_vid 
			+ rezalt.w2_antiquity_veb
			+ rezalt.w2_score_m_sr_simv_veb_bki
			+ rezalt.w2_score_n_sr_simv_veb
			+ rezalt.w2_sd
			+ rezalt.w2_speeddynam_12
			+ rezalt.w2_stag_in_months
			+ rezalt.w2_sex_age_derivative
			+ rezalt.w2_summ_aggregate
		WHEN rezalt.SC_id_20_28=26
		THEN ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki) /*ок*/
			+ rezalt.w2_dynamics_quality_bki /*ок*/
			+ rezalt.w2_education /*ок*/
			+ rezalt.w2_od_sum_veb_all /*ок*/
			+ rezalt.w2_org_vid /*ок*/
			+ rezalt.w2_dynamics_charge_bki /*ок*/
			+ rezalt.w2_format_groups /*ок*/
			+ rezalt.w2_score_n_sr_simv_veb /*ок*/
			+ rezalt.w2_sd /*ок*/
			+ rezalt.w2_stag_in_months /*ок*/
			+ rezalt.w2_sum_12_rate /*ок*/
			+ rezalt.w2_region_gp /*пффф! было w2_scorecard_region_group_v2p в тз. Сколько можно?!!*/
			+ rezalt.w2_summ_aggregate /*ок*/
		WHEN rezalt.SC_id_20_28=27
		THEN ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki)
			+ rezalt.w2_dynamics_charge_bki
			+ rezalt.w2_dynamics_quality_veb
			+ rezalt.w2_education 
			+ rezalt.w2_household
			+ rezalt.w2_od_sum_veb_all
			+ rezalt.w2_org_vid
			+ rezalt.w2_period
			+ rezalt.w2_defolt_veb
			+ rezalt.w2_sd
			+ rezalt.w2_speeddynam_12
			+ rezalt.w2_monthswentwhenlastdeliq
			+ rezalt.w2_sex_age_derivative
		WHEN rezalt.SC_id_20_28=28
		THEN ifn(rezalt.w2_qm_new_bki=., 0, rezalt.w2_qm_new_bki) /*ок*/
			+ rezalt.w2_INQUIRER_POINT /*ок*/
			+ rezalt.w2_dynamics_charge_bki /*ок*/
			+ rezalt.w2_od_sum_veb_all /*ок*/
			+ rezalt.w2_org_vid /*ок*/
			+ rezalt.w2_period /*ок*/
			+ rezalt.w2_score_n_sr_simv_veb /*ок*/
			+ rezalt.w2_sd /*ок*/
			+ rezalt.w2_sum_12_rate /*ок*/
			+ rezalt.w2_monthswentwhenlastdeliq /*ок*/
			+ rezalt.w2_region_gp /*пффф! было w2_scorecard_region_group_v2p в тз. Сколько можно?!!*/
			+ rezalt.w2_summ_aggregate /*ок*/
		END) as r_mark_20_28
	, (CASE 
		WHEN rezalt.SC_id_30_38=30 
			THEN SUM( rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_CR_HIST_BKI 
				,rezalt.wG9_D 
				,rezalt.wG9_delay_day_y_bki
				,rezalt.wG9_EDUCATION 
				,rezalt.wG9_HOUSEHOLD 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_SALE_ID
				,rezalt.wG9_SCORE_N_SR_SIMV_bki 
				,rezalt.wG9_STAG_in_months
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_sex1_age)
		WHEN rezalt.SC_id_30_38=31 
			THEN SUM( rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_D 
				,rezalt.wG9_delay_day_y_bki
				,rezalt.wG9_EDUCATION 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_SALE_ID
				,rezalt.wG9_SCORE_N_SR_SIMV_bki 
				,rezalt.wG9_STAG_in_months
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_sex1_age 
				,rezalt.wG9_CHARGE_3_12 )
		WHEN rezalt.SC_id_30_38=32
			THEN SUM( rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_D 
				,rezalt.wG9_delay_day_y_bki
				,rezalt.wG9_HOUSEHOLD 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_SALE_ID
				,rezalt.wG9_SCORE_N_SR_SIMV_bki 
				,rezalt.wG9_STAG_in_months
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_sex1_age 
				,rezalt.wG9_CHARGE_3_12 
				,rezalt.wG9_ANTIQUITY_bki )
		WHEN rezalt.SC_id_30_38=33
			THEN SUM( rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_CR_HIST_BKI 
				,rezalt.wG9_D 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_STAG_in_months
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_DELAY_ALL_DAY_6M_VEB 
				,rezalt.wG9_OD_sv_ras
				,rezalt.wG9_SPEEDDYNAM_12 
				,rezalt.wG9_TIME_FROM_DELAY_BKI )
		WHEN rezalt.SC_id_30_38=34
			THEN SUM( rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_DELAY_ALL_DAY_6M_VEB
				,rezalt.wG9_OD_sv_ras
				,rezalt.wG9_TIME_FROM_DELAY_BKI 
				,rezalt.wG9_CNT_CREDIT_VEB_BKI 
				,rezalt.wG9_SD 
				,rezalt.wG9_region_simb1 )
		WHEN rezalt.SC_id_30_38=35
			THEN SUM(rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_EDUCATION 
				,rezalt.wG9_HOUSEHOLD 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_STAG_in_months
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_sex1_age 
				,rezalt.wG9_SD 
				,rezalt.wG9_DELAY_1_89_NUM_BKI 
				,rezalt.wG9_MAX_12_rate 
				,rezalt.wG9_SCORE_M_SR_SIMV_bki 
				,rezalt.wG9_SCORE_N_SR_SIMV_veb )
		WHEN rezalt.SC_id_30_38=36 
			THEN SUM(rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_STAG_in_months
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_OD_sv_ras
				,rezalt.wG9_SD 
				,rezalt.wG9_region_simb1
				,rezalt.wG9_SCORE_N_SR_SIMV_veb
				,rezalt.wG9_DYNAMICS_QUALITY_bki 
				,rezalt.wG9_MONTHSWENTWHENLASTDELIQ1
				,rezalt.wG9_SUMMA_DEBT_BKI_RATE_MATCHER 
				,rezalt.wG9_sovokup )
		WHEN rezalt.SC_id_30_38=37 
			THEN SUM(rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_sex1_age 
				,rezalt.wG9_SPEEDDYNAM_12 
				,rezalt.wG9_TIME_FROM_DELAY_BKI 
				,rezalt.wG9_SD 
				,rezalt.wG9_region_simb1
				,rezalt.wG9_DEBT_all 
				,rezalt.wG9_DEFOLT_bki
				,rezalt.wG9_DYNAMICS_QUALITY_veb )
		WHEN rezalt.SC_id_30_38=38
			THEN SUM(rezalt.wG9_CNT_REJECTS 
				,rezalt.wG9_ORG_VID
				,rezalt.wG9_QM_new_bki 
				,rezalt.wG9_closed_debt_max_bki
				,rezalt.wG9_loan_freq_bki
				,rezalt.wG9_OD_sv_ras
				,rezalt.wG9_SD 
				,rezalt.wG9_region_simb1
				,rezalt.wG9_SCORE_N_SR_SIMV_veb
				,rezalt.wG9_MONTHSWENTWHENLASTDELIQ1
				,rezalt.wG9_DEBT_all 
				,rezalt.wG9_CNT_CREDIT_VEB 
				,rezalt.wG9_PERIOD )
		END) as r_mark_g9_30_38
/*
-3.4943+0.0349
-3.7488+0.0342
-2.4290+0.0252
-4.6015+0.0343
-3.9349+0.0350
-5.2265+0.0351
-4.6024+0.0345
-5.7458+0.0343
-5.0734+0.0348

*/
		,ROUND((CASE 
			WHEN sc_ID_30_38=30 THEN (1/(1+EXP(-3.4943+0.0349*(CALCULATED r_mark_g9_30_38)))) 
			WHEN sc_ID_30_38=31 THEN (1/(1+EXP(-3.7488+0.0342*(CALCULATED r_mark_g9_30_38)))) 
			WHEN sc_ID_30_38=32 THEN (1/(1+EXP(-2.4290+0.0252*(CALCULATED r_mark_g9_30_38)))) 
			WHEN sc_ID_30_38=33 THEN (1/(1+EXP(-4.7251+0.0348 /*-4.6015+0.0343*/*(CALCULATED r_mark_g9_30_38))))
			WHEN sc_ID_30_38=34 THEN (1/(1+EXP(-3.9349+0.0350*(CALCULATED r_mark_g9_30_38))))
			WHEN sc_ID_30_38=35 THEN (1/(1+EXP(-5.2265+0.0351*(CALCULATED r_mark_g9_30_38))))
			WHEN sc_ID_30_38=36 THEN (1/(1+EXP(-4.6024+0.0345*(CALCULATED r_mark_g9_30_38))))
			WHEN sc_ID_30_38=37 THEN (1/(1+EXP(-5.809+0.0346 /*-5.7458+0.0343*/*(CALCULATED r_mark_g9_30_38))))
			WHEN sc_ID_30_38=38 THEN (1/(1+EXP(-5.0734+0.0348*(CALCULATED r_mark_g9_30_38))))
			ELSE . END), .0001)*100
		as pd_30_38_G9
		
	FROM (SELECT t1.*,
					(CASE WHEN t1.SC_id_new in(12,13) AND reg_grp_channel=. THEN 15
					WHEN t1.SC_id_new in(12,13) AND reg_grp_channel=2 THEN 6
					WHEN t1.SC_id_new in(12,13) AND reg_grp_channel=1 THEN 1
					WHEN t1.SC_id_new in(12,13) AND reg_grp_channel=3 THEN 15
					WHEN t1.SC_id_new in(12,13) THEN 15
					WHEN t1.SC_id_new in(14,15) AND reg_grp_channel=. THEN 14
					WHEN t1.SC_id_new in(14,15) AND reg_grp_channel IN (1,2) THEN 8
					WHEN t1.SC_id_new in(14,15) AND reg_grp_channel=3 THEN 14
					WHEN t1.SC_id_new in(14,15) THEN 14
					WHEN t1.SC_id_new in(16,17) AND reg_grp_channel=. THEN 15
					WHEN t1.SC_id_new in(16,17) AND reg_grp_channel IN(1) THEN 9
					WHEN t1.SC_id_new in(16,17) AND reg_grp_channel IN(2) THEN 15
					WHEN t1.SC_id_new in(16,17) THEN 15
				END) as w_reg_grp_channel,
				(CASE WHEN t1.SC_id_new in(12,13) AND srok_sovocup=. THEN 12
					WHEN t1.SC_id_new in(12,13) AND srok_sovocup=2 THEN 7
					WHEN t1.SC_id_new in(12,13) AND srok_sovocup=1 THEN 2
					WHEN t1.SC_id_new in(12,13) AND srok_sovocup=3 THEN 12
					WHEN t1.SC_id_new in(12,13) AND srok_sovocup IN(4,5) THEN 24
					WHEN t1.SC_id_new in(12,13) THEN 12
					WHEN t1.SC_id_new in(14,15) AND srok_sovocup=. THEN 13
					WHEN t1.SC_id_new in(14,15) AND srok_sovocup=3 THEN 13
					WHEN t1.SC_id_new in(14,15) AND srok_sovocup=1 THEN -3
					WHEN t1.SC_id_new in(14,15) AND srok_sovocup=2 THEN 5
					WHEN t1.SC_id_new in(14,15) AND srok_sovocup IN(4,5) THEN 29
					WHEN t1.SC_id_new in(14,15) THEN 13
					WHEN t1.SC_id_new in(16,17) AND srok_sovocup=. THEN 24
					WHEN t1.SC_id_new in(16,17) AND srok_sovocup=2 THEN 4
					WHEN t1.SC_id_new in(16,17) AND srok_sovocup=3 THEN 13
					WHEN t1.SC_id_new in(16,17) AND srok_sovocup=1 THEN -3
					WHEN t1.SC_id_new in(16,17) AND srok_sovocup IN(4,5) THEN 24
					WHEN t1.SC_id_new in(16,17) THEN 24
				END) as w_srok_sovocup,
				(CASE WHEN t1.SC_id_new in(12,13) AND k_od=. THEN 12
					WHEN t1.SC_id_new in(12,13) AND k_od=2 THEN 12
					WHEN t1.SC_id_new in(12,13) AND k_od=1 THEN -6
					WHEN t1.SC_id_new in(12,13) AND k_od=3 THEN 32
					WHEN t1.SC_id_new in(12,13) THEN 12
					WHEN t1.SC_id_new in(16,17) AND k_od=. THEN 2
					WHEN t1.SC_id_new in(16,17) AND k_od=2 THEN 20
					WHEN t1.SC_id_new in(16,17) AND k_od=1 THEN 2
					WHEN t1.SC_id_new in(16,17) AND k_od=3 THEN 33
					WHEN t1.SC_id_new in(16,17) THEN 12
				END) as w_k_od
			FROM trakhach._REZ_201210_201406_new t1) AS rezalt;

DROP TABLE trakhach._REZ_201210_201406_new;
quit;


*=========Макрообъявления для отправления сообщений==========;
%macro M_Singature;
	Put "</br>";
	Put "</br>";
	Put "С Уважением,</br>";
	Put "Трахачев Вячеслав Валерьевич </br>";
	Put "тел. внутренний 37443 </br>";
%mend M_Singature;

%macro SendEmail(fromUser=vvtrahachev@orient.root.biz, mes_about_rezult=);
	FileName SENDMAIL Email FROM="&fromUser." TYPE='text/html'
		to=("vvtrahachev@orient.root.biz" /*"nvlyalin@express-bank.ru" "pvprostyakov@express-bank.ru"*/)
		SUBJECT="Результат обновления _REZ_201210_201407_R_NEW %sysfunc(date(),ddmmyy9.)";

	Data _NULL_;
		File SENDMAIL;

		Put "Добрый день! </br>";
		Put "&mes_about_rezult." "</br>";
		%M_Singature;
	Run;
%mend SendEmail;

*%SendEmail(fromUser=SAS_Trakhachev_V
	, mes_about_rezult=Обновлена таблица trakhach._REZ_201210_201407_R_NEW для карт g9);
