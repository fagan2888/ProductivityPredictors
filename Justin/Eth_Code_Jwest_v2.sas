libname X 'C:\Users\west_j\Desktop\eth_shit';


/*  This pulls in the CSVs from a directory on my local machine.  You update the macro with the number and the pre-fix and it will import that 
as  a SAS Dataset*/
%let num=7_hh ;




PROC IMPORT OUT= X.sect&num.
            DATAFILE= "C:\Users\west_j\Desktop\eth_shit\sect&num._w3.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

/*  First lets subset the HH stuff*/


/*sect cover subset*/


proc sql;
		create table x.sect_cover_cc_SUB_2

		as select  distinct  

					household_id2, 
					saq01 as region_code,
					saq02 as zone_code,
					ph_saq10 as household_size

		from x.sect_cover_cc

;

quit;


/*subset Sect4HH*/



proc sql;
		create table x.sect4_hh_SUB_2
		as select  distinct  

					individual_id2,
					household_id2, 
					saq01 as region_code,
					saq02 as zone_code,
					hh_s4q04 as hours_per_week_agg,
					hh_s4q04/7 as hours_per_day_agg,
					hh_s4q05 as hours_per_week_non_agg,
					hh_s4q05/7 as hours_per_day_non_agg,
					hh_s4q06 as hours_per_week_other_jobs,
					hh_s4q06/7 as hours_per_day_other_jobs,
					hh_s4q02_a as hours_per_day_collect_water,
					hh_s4q02_a*7 as hours_per_week_collect_water,
					pw_w3 as HouseHoldWeight

		from x.sect4_hh
	
;
quit;


/**/
/**/
/*proc sql;*/
/*		create table x.sect4_hh_SUB_2_test*/
/*		as select  distinct  *,*/
/**/
/*					individual_id2,*/
/*					household_id2, */
/*					region_code,*/
/*					zone_code,*/
/*					sum(individual_count) as household_size_Real,*/
/*					sum(hours_per_week_agg) as sum_hours_per_week_agg,*/
/*					sum(hours_per_day_agg) as sum_hours_per_day_agg,*/
/*					sum(hours_per_week_non_agg) as sum_hours_per_week_non_agg,*/
/*					sum(hours_per_day_non_agg) as sum_hours_per_day_non_agg,*/
/*					sum(hours_per_week_other_jobs) as sum_hours_per_week_other_jobs,*/
/*					sum(hours_per_day_other_jobs) as sum_hours_per_day_other_jobs,*/
/*					sum(hours_per_day_collect_water) as sum_hours_per_day_collect_water,*/
/*					sum(hours_per_week_collect_water*7) as sum_hours_per_week_collect_water,*/
/*					HouseHoldWeight*/
/**/
/*		from x.sect4_hh_SUB_2*/
/**/
/*		group by household_id2*/
/*;*/
/*quit;*/








proc sql;
		create table x.sect4b_hh_SUB_2
		as select  distinct  

					household_id2, 
					Individual_id2,
					saq01 as region_code,
					saq02 as zone_code,
					hh_s4bq02 as bank_account

		from x.sect4b_hh
;
quit;


/*subset Sect3HH*/
proc sql;
		create table x.sect2_hh_SUB_2
		as select   distinct 

 					household_id2, 
 					Individual_id2,
 					saq01 as region_code,
 					saq02 as zone_code,
					hh_s2q05 as Highest_Grade_Completed

		from x.sect2_hh

;

quit;




/*Lets subset the PP down*/

/*sect3_pp subset*/

proc sql;
		create table x.sect3_pp_SUB_2
		as select  distinct  

					household_id2, 
					field_id,
					saq01 as region_code,
					saq02 as zone_code,
					pp_s3q28_a as Num_of_Hired_Men,
					pp_s3q28_d as Num_of_Hired_Women,
					pp_s3q14 as Use_Fertilizer,
					pp_s3q21 as Use_Manure_Fertilizer

		from x.sect3_pp;



quit;



/*sect4_pp subset*/

proc sql;
		create table x.sect4_pp_SUB_2
		as select distinct    

				   household_id2, 
				   field_id,
                   pp_s4q01_a as crop_name,
                   pp_s4q01_b as crop_code,
                   pp_s4q02 as Type_of_Planting,
                   pp_s4q03 as Amount_of_field_planted,
                   saq01 as region_code,
                   saq02 as zone_code

		from x.sect4_pp;


quit;




/*sect7_pp subset*/

proc sql;
		create table x.sect7_pp_SUB_2
		as select  distinct  

					household_id2, 
					saq01 as region_code,
					saq02 as zone_code,
					pp_s7q02 as chemical_fertilizers_used	,
					pp_s7q03 as reason_no_chemicals_used	,
					pp_s7q04 as extension_program	,
					pp_s7q06 as Credit_Services	,
					pp_s7q08 as advisory_Services	,
					pp_s7q01 as Crop_Rotation	,
					pp_s7q21 as who_in_charge_of_crops	,
					pp_s7q26 as who_decides_crops_2_sell	,
					pp_s7q27 as who_takes_care_of_Crops	,
					pp_s7q31 as who_decides_on_use_of_income	,
					pp_s7q33 as who_decides_on_use_of_crops	,
					pp_s7q22 as who_buys_agg_supplies	

		from x.sect7_pp;
quit;


/* now lets subset the pH stuff*/

proc sql;
		create table x.sect7_hh_SUB_2
		as select  distinct  

					household_id2, 
					saq01 as region_code,
					saq02 as zone_code,
					hh_s7q01 as worry_enough_food_to_eat_weekly	

		from x.sect7_hh;
quit;


/*sect9_ph subset*/

proc sql;
		create table x.sect9_ph_SUB_2
		as select distinct  

					household_id2, 
					saq01 as region_code,
					saq02 as zone_code,
					field_id,
					crop_name	,
					crop_code	,
					ph_s9q03 as  did_you_harvest_crop_from_field	,
					ph_s9q04_a	as amt_of_crop_harvest,
					ph_s9q04_b	as amt_of_crop_harvest_units

		from x.sect9_ph;
quit;

/*sect10_ph subset*/

proc sql;
		create table x.sect10_ph_SUB_2
		as select distinct   


					household_id2, 
					field_id,
					saq01 as region_code,
					saq02 as zone_code,
					crop_name	,
					crop_code	,
					ph_s10q01_a as amt_days_men_hired	,
					ph_s10q01_c as daily_wage_per_man	,
					ph_s10q01_d	as amt_days_women_hired,
					ph_s10q01_f	 as daily_wage_per_woman,
					ph_s10q01_g	as amt_days_children_hired,
					ph_s10q01_i	as daily_wage_per_child

		from x.sect10_ph;


quit;

/*sect11_ph subset*/

proc sql;
		create table x.sect11_ph_SUB_2
		as select distinct  


					household_id2, 
					saq01 as region_code,
					saq02 as zone_code,
					crop_name,
					crop_code,
					ph_s11q01 as  Did_You_sell_harvest_crop,
					ph_s11q04 as value_of_Crop_sales





		from x.sect11_ph;
quit;



/*sect1_hh subset*/

proc sql;
		create table x.sect1_hh_SUB_2
		as select distinct  

					household_id2,  
					individual_id2,
					saq01 as region_code,
					saq02 as zone_code,
					hh_s1q02 as  hh_member_relationship,
					hh_s1q03 as sex_of_hh_member,
					hh_s1q04a as age_of_hh_member,
					hh_s1q08 as marital_status
			

		from x.sect1_hh;
quit;



/* Now lets do some joins*/

proc sql;
		create table eth_join_1
		as select distinct * 

		from x.sect3_pp_SUB_2 as a inner join x.sect4_pp_SUB_2 as b

				on  a.household_id2=b.household_id2
				and a.field_id=b.field_id 
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code;
quit;



proc sql;
		create table eth_join_3
		as select   distinct * 

		from eth_join_1 as a inner join x.sect7_pp_SUB_2 as b
				on  a.household_id2=b.household_id2 
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code;
quit;

proc sql;
		create table eth_join_4
		as select  distinct * 

		from eth_join_3 as a inner join x.sect9_ph_SUB_2 as b
				on  a.household_id2=b.household_id2 
				and a.field_id=b.field_id and a.crop_code=b.crop_code 
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code;
quit;

proc sql;
		create table eth_join_5
		as select distinct   * 

		from eth_join_4 as a inner join x.sect10_ph_SUB_2 as b
				on  a.household_id2=b.household_id2 
				and a.field_id=b.field_id and a.crop_code=b.crop_code
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code;


quit;

proc sql;
		create table eth_join_6
		as select  distinct * 

		from eth_join_5 as a inner join x.sect11_ph_SUB_2 as b
				on  a.household_id2=b.household_id2 
				and a.crop_code=b.crop_code
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code;
quit;


proc sql;
		create table eth_join_6
		as select  distinct * 

		from eth_join_6 as a inner join x.sect_cover_cc_SUB_2 as b
				on  a.household_id2=b.household_id2 
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code;
quit;


proc sql;
		create table eth_join_6
		as select  distinct * 

		from eth_join_6 as a inner join x.sect7_hh_SUB_2 as b
				on  a.household_id2=b.household_id2 
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code;
quit;



/*I am going to join the Individual stuff first*/


proc sql;
		create table X.ETH_join_indiv_1
		as select  distinct *
		from x.sect2_hh_SUB_2 as a inner join x.sect1_hh_SUB_2 as b
				on  a.individual_id2=b.individual_id2 
				and a.household_id2=b.household_id2 
  				and a.region_code=b.region_code
				and a.zone_code=b.zone_code;

quit;


proc sql;
		create table X.ETH_join_indiv_2
		as select  distinct *
		from X.ETH_join_indiv_1 as a left join x.sect4_hh_SUB_2 as b
				on  a.individual_id2=b.individual_id2 
				and a.household_id2=b.household_id2 
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code
		;

quit;

proc sql;
		create table X.ETH_join_indiv_3
		as select  distinct *




		from X.ETH_join_indiv_2 as a left join x.sect4b_hh_SUB_2 as b
				on  a.individual_id2=b.individual_id2 
				and a.household_id2=b.household_id2 
				and a.region_code=b.region_code
				and a.zone_code=b.zone_code

;


quit;

proc sql;
create table x.eth_join_indiv_3
as select *


from x.eth_join_indiv_3;



quit;


proc sql;
		create table x.ethiopia_final_distinct_female
		as select distinct *


		from X.ETH_join_indiv_3 as a inner join eth_join_6 as b
				on a.household_id2=b.household_id2 
				where sex_of_hh_member=2
			
;


quit;






data 	x.all_individual_eth_Data_female (drop=household_id2 individual_id2 region_code zone_code hh_member_relationship 
hours_per_day_agg hours_per_Day_non_agg hours_per_Week_other_jobs hours_per_day_other_jobs hours_per_day_collect_water hours_per_Week_collect_water householdweight fieldid cropcode cropname reason_no_chemicals_used cat_hours_per_week_agg cat_hours_per_Day_non_agg  cat_hours_per_day_other_jobs cat_hours_per_Day_collect_water male);
	set x.ethiopia_final_distinct_female;

					if highest_grade_completed not in (0,.) then highest_grade_completed=1;
					else highest_grade_completed=0 ;

					if hours_per_day_agg>=8 then  Cat_Hours_per_day_agg = 1;
					if hours_per_day_agg<8 then  Cat_Hours_per_day_agg = 0;

					if hours_per_day_non_agg>=8 then  Cat_Hours_per_day_non_agg = 1;
					if hours_per_day_non_agg<8 then  Cat_Hours_per_day_non_agg = 0;

					if hours_per_day_other_jobs>=8 then  Cat_hours_per_day_other_jobs = 1;
					if hours_per_day_other_jobs<8 then  Cat_hours_per_day_other_jobs = 0;

					if hours_per_day_collect_water>=8 then  Cat_hours_per_day_collect_water = 1;
					if hours_per_day_collect_water<8 then  Cat_hours_per_day_collect_water = 0;

					if region_code=1 then region_code_1=1; 
					ELSE region_code_1=0;
					if region_code=2 then region_code_2=1; 
				ELSE region_code_2=0;
					if region_code=3 then region_code_3=1 ;
					ELSE region_code_3=0;
					if region_code=4 then region_code_4=1; 
					ELSE region_code_4=0;
					if region_code=5 then region_code_5=1 ;
					ELSE region_code_5=0;
					if region_code=6 then region_code_6=1 ;
					ELSE region_code_6=0;
					if region_code=7 then region_code_7=1 ;
					ELSE region_code_7=0;
					if region_code=12 then region_code_12=1 ;
					ELSE region_code_12=0;
					if region_code=13 then region_code_13=1 ;
					ELSE region_code_13=0;
					if region_code=14 then region_code_14=1; 
					ELSE region_code_14=0;

					if marital_status=2 then marital_status=1 ;
					else marital_status=0;

					if bank_account=1 then bank_account=1 ;
					else bank_account=0;

					if  Num_of_Hired_men>0 then hired_men=1 ;
					else hired_men=0;

					if Num_of_Hired_Women>0 then hired_women=1 ;
					else hired_women=0;

					if use_fertilizer=1 then use_fertilizer=1 ;

					else use_fertilizer=0;

					if use_manure_fertilizer=1 then use_manure_fertilizer=1;

					else use_manure_fertilizer=0;
					
					if chemical_fertilizers_used=1 then chemical_fertilizers_used=1;

					else chemical_fertilizers_used=0;

					if extension_program=1 then extension_program=1 ;
					else extension_program=0;

					if credit_services=1 then credit_services=1 ;
					else credit_services=0;

					if advisory_services=1 then advisory_services=1;
					else advisory_services=0;

					if crop_rotation=1 then crop_rotation=1;
					else crop_rotation=0;

					if who_in_charge_of_crops in (2,4) then who_in_charge_of_crops=1;
					else who_in_charge_of_crops=0;
					if who_decides_crops_2_sell in (2,4) then who_decides_crops_2_sell=1;
					else who_decides_crops_2_sell= 0;	
					if who_takes_care_of_Crops	in (2,4) then who_takes_care_of_Crops=1;
					else who_takes_care_of_Crops=0;
					if who_decides_on_use_of_income in (2,4) then who_decides_on_use_of_income=1;
					else who_decides_on_use_of_income=0;
					if who_decides_on_use_of_crops in (2,4) then who_decides_on_use_of_crops=1;	
					else who_decides_on_use_of_crops=0;

					if who_buys_agg_supplies in (2,4) then who_buys_agg_supplies=1;	
					else who_buys_agg_supplies=0;

					if sex_of_hh_member=2 then female=1;
					

					
					if sex_of_hh_member=1 then male=1;

					if harvest_crop_from_field =1 then harvest_crop_from_field =1;	
					else harvest_crop_from_field =0;


					if did_you_sell_harvest_crop =1 then did_you_sell_harvest_crop =1;	
					else did_you_sell_harvest_crop =0;


run;

data shit;
set x.all_individual_eth_Data;
where cat_hours_per_Day_agg=1;
run;



proc sql;
		create table x.ethiopia_final_distinct_ALL
		as select distinct *




					

		from X.ETH_join_indiv_3 as a inner join eth_join_6 as b
				on a.household_id2=b.household_id2 


		
;


quit;


data 	x.ethiopia_final_distinct_ALL_2 (drop=household_id2 individual_id2 region_code zone_code hh_member_relationship 
hours_per_day_agg hours_per_Day_non_agg field_id hours_per_Week_other_jobs hours_per_day_other_jobs 
hours_per_day_collect_water hours_per_Week_collect_water householdweight fieldid cropcode amt_days_men_hired daily_wage_per_man amt_days_women_hired daily_wage_per_woman amt_days_children_hired daily_wage_per_child cropname reason_no_chemicals_used cat_hours_per_week_agg cat_hours_per_Day_non_agg  cat_hours_per_day_other_jobs cat_hours_per_Day_collect_water sex_of_hh_member);
	set x.ethiopia_final_distinct_ALL;

					if highest_grade_completed not in (0,.) then highest_grade_completed=1;
					else highest_grade_completed=0 ;

					if hours_per_day_agg>=8 then  Cat_Hours_per_day_agg = 1;
					if hours_per_day_agg<8 then  Cat_Hours_per_day_agg = 0;

					if hours_per_day_non_agg>=8 then  Cat_Hours_per_day_non_agg = 1;
					if hours_per_day_non_agg<8 then  Cat_Hours_per_day_non_agg = 0;

					if hours_per_day_other_jobs>=8 then  Cat_hours_per_day_other_jobs = 1;
					if hours_per_day_other_jobs<8 then  Cat_hours_per_day_other_jobs = 0;

					if hours_per_day_collect_water>=8 then  Cat_hours_per_day_collect_water = 1;
					if hours_per_day_collect_water<8 then  Cat_hours_per_day_collect_water = 0;

					if region_code=1 then region_code_1=1; 
					ELSE region_code_1=0;
					if region_code=2 then region_code_2=1; 
				ELSE region_code_2=0;
					if region_code=3 then region_code_3=1 ;
					ELSE region_code_3=0;
					if region_code=4 then region_code_4=1; 
					ELSE region_code_4=0;
					if region_code=5 then region_code_5=1 ;
					ELSE region_code_5=0;
					if region_code=6 then region_code_6=1 ;
					ELSE region_code_6=0;
					if region_code=7 then region_code_7=1 ;
					ELSE region_code_7=0;
					if region_code=12 then region_code_12=1 ;
					ELSE region_code_12=0;
					if region_code=13 then region_code_13=1 ;
					ELSE region_code_13=0;
					if region_code=14 then region_code_14=1; 
					ELSE region_code_14=0;

					if marital_status=2 then marital_status=1 ;
					else marital_status=0;

					if bank_account=1 then bank_account=1 ;
					else bank_account=0;

					if  Num_of_Hired_men>0 then hired_men=1 ;
					else hired_men=0;

					if Num_of_Hired_Women>0 then hired_women=1 ;
					else hired_women=0;

					if use_fertilizer=1 then use_fertilizer=1 ;

					else use_fertilizer=0;

					if use_manure_fertilizer=1 then use_manure_fertilizer=1;

					else use_manure_fertilizer=0;
					
					if chemical_fertilizers_used=1 then chemical_fertilizers_used=1;

					else chemical_fertilizers_used=0;

					if extension_program=1 then extension_program=1 ;
					else extension_program=0;

					if credit_services=1 then credit_services=1 ;
					else credit_services=0;

					if advisory_services=1 then advisory_services=1;
					else advisory_services=0;

					if crop_rotation=1 then crop_rotation=1;
					else crop_rotation=0;

					if who_in_charge_of_crops in (2,4) then who_in_charge_of_crops=1;
					else who_in_charge_of_crops=0;
					if who_decides_crops_2_sell in (2,4) then who_decides_crops_2_sell=1;
					else who_decides_crops_2_sell= 0;	
					if who_takes_care_of_Crops	in (2,4) then who_takes_care_of_Crops=1;
					else who_takes_care_of_Crops=0;
					if who_decides_on_use_of_income in (2,4) then who_decides_on_use_of_income=1;
					else who_decides_on_use_of_income=0;
					if who_decides_on_use_of_crops in (2,4) then who_decides_on_use_of_crops=1;	
					else who_decides_on_use_of_crops=0;

					if who_buys_agg_supplies in (2,4) then who_buys_agg_supplies=1;	
					else who_buys_agg_supplies=0;

					if sex_of_hh_member=2 then female=1;
					

					
					if sex_of_hh_member=1 then male=1;

					if harvest_crop_from_field =1 then harvest_crop_from_field =1;	
					else harvest_crop_from_field =0;


					if did_you_sell_harvest_crop =1 then did_you_sell_harvest_crop =1;	
					else did_you_sell_harvest_crop =0;


run;




PROC SQL;
CREATE TABLE CHECKS
AS SELECT DISTINCT *
FROM x.ethiopia_final_distinct_FEMALE
WHERE HOURS_PER_DAY_AGG NOT IN (0,.) ;
QUIT;

PROC FREQ DATA=checks;

TABLE HOURS_PER_week_AGG ;

RUN;

proc reg;
      model hours_per_Day_agg = hh_member_relationship	sex_of_hh_member	age_of_hh_member	marital_status	hours_per_week_agg	hours_per_day_agg	
hours_per_week_non_agg	hours_per_day_non_agg	hours_per_week_other_jobs	hours_per_day_other_jobs	hours_per_day_collect_water	hours_per_week_collect_water	HouseHoldWeight	bank_account	field_id	Num_of_Hired_Men	Num_of_Hired_Women	Use_Fertilizer	Use_Manure_Fertilizer	crop_name	crop_code	Type_of_Planting	Amount_of_field_planted	chemical_fertilizers_used	reason_no_chemicals_used	extension_program	Credit_Services	advisory_Services	Crop_Rotation	who_in_charge_of_crops	who_decides_crops_2_sell	who_takes_care_of_Crops	who_decides_on_use_of_income	who_decides_on_use_of_crops	who_buys_agg_supplies	did_you_harvest_crop_from_field	amt_of_crop_harvest	amt_of_crop_harvest_units	amt_days_men_hired	daily_wage_per_man	amt_days_women_hired	daily_wage_per_woman	amt_days_children_hired	daily_wage_per_child	Did_You_sell_harvest_crop	value_of_Crop_sales	household_size	worry_enough_food_to_eat_weekly	Cat_Hours_per_day_agg	Cat_Hours_per_day_non_agg	Cat_hours_per_day_other_jobs	Cat_hours_per_day_collect_water	region_code_1	region_code_2	region_code_3	region_code_4	region_code_5	region_code_6	region_code_7	region_code_12	region_code_13	region_code_14	hired_men	hired_women	female	male	harvest_crop_from_field
;
   run;




PROC EXPORT DATA= x.all_individual_eth_Data_female
            OUTFILE= "C:\Users\west_j\Desktop\eth_shit\Ethiopia_final_fe
male_dataset.csv" 
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;


PROC EXPORT DATA= X.ETHIOPIA_FINAL_DISTINCT_ALL 
            OUTFILE= "C:\Users\west_j\Desktop\eth_shit\Ethiopia_final_ALL_dataset.csv" 
            DBMS=CSV REPLACE;
     PUTNAMES=YES;
RUN;

/* This stuff is for  maps trying to bring in Latitude and Long*/
/*
data x.ethiopia_final_distinct_female;
set x.ethiopia_final_distinct_female;

if region_code=1 then Province= '53';
if region_code=2 then Province= '45';
if region_code=3 then Province= '46';
if region_code=4 then Province= '51';
if region_code=5 then Province= '52';
if region_code=6 then Province= '47';
if region_code=12 then Province= '49';
if region_code=13 then Province= '50';
if region_code=14 then Province= '44';
if region_code=15 then Province= '48';
run;

proc sql;
create table x.ethiopia_final_female_geo
as select  a.*, b.*
from x.ethiopia_final_distinct_female as a left join mapssas.ethiopi2 as b
on a.province=put(b.ID,2.)
;
quit;


proc sql;
create table x.ethiopia_final_female_geo_out
as select distinct a.*,b.x, b.y
from x.ethiopia_final_female_geo as a inner join mapssas.ethiopia b
on a.id=b.id;
quit;




/*run check to see if we have duplicates*/

proc sql;
create table test
as select distinct  individual_id2, household_id2, crop_code
from x.ethiopia_final_distinct_female;
quit;



