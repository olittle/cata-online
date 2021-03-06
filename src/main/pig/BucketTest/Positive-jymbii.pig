---.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
--
-- File Name : Positive.pig
--
-- Purpose :
--
-- Creation Date : 26-07-2013
--
-- Last Modified : Sun 28 Jul 2013 01:53:09 AM PDT
--
-- Created By : Huan Gui (hgui@linkedin.com) 
--
--_._._._._._._._._._._._._._._._._._._._._.

%declare kernel   `uname -s`
%default now      `date "+%s"`
%default daysago  1
%declare tzoffset `bash -c 'if [[ "$kernel" == "Linux" ]]; then echo "-28800";  else echo "0"; fi'`
%declare tzero    `echo $(($now - 60 * 60 * 24 * $daysago + $tzoffset))`

%declare REPORT_DATE `bash -c 'if [[ "$kernel" == "Linux" ]]; then date -d "@$tzero" "+%Y/%m/%d"; else date -r $tzero "+%Y/%m/%d"; fi'`
%declare e `echo $REPORT_DATE`

applypair = LOAD '/data/tracking/JobApplyClickEvent' USING LiAvroStorage('date.range','num.days=1;days.ago=$daysago;error.on.missing=false');
applypair = foreach applypair generate header.memberId as memberId, jobId, header.time as time;

member = load '$OUTPUT/JYMBII-batch/MemberList' USING BinaryJSON();

applypair = join applypair by memberId, member by memberId parallel 2000; 
applypair = foreach applypair generate applypair::memberId as memberId, applypair::jobId as jobId, applypair::time as time; 

-- user-job pairs come from JYMBII
events = LOAD '/data/tracking/PageViewEvent' USING LiAvroStorage('date.range','num.days=1;days.ago=$daysago;error.on.missing=false');

-- get all events for job views
job_view_events = FILTER events BY requestHeader.pageKey == 'jobs_seeking_view_job' AND header.memberId > 0;

job_views = FOREACH job_view_events GENERATE 
  (int)header.memberId   AS memberId,
  (long)header.time      AS time,
  trackingCode,
  (int)trackingInfo#'0'  AS jobId;

job_views = join job_views by memberId, member by memberId parallel 2000; 
job_views = foreach job_views generate job_views::memberId as memberId, job_views::time as time, job_views::trackingCode as trackingCode, job_views::jobId as jobId; 

job_applications_j4u = FILTER job_views BY (trackingCode == 'rj_em') OR
                (trackingCode == 'rj_grps') OR
                (trackingCode == 'nmp_rj_job') OR
                (trackingCode == 'rj_jshp') OR
                (trackingCode == 'jobs_seeking_home') OR
                (trackingCode == 'jobs_biz_prem_jymbii') OR
                (trackingCode == 'EML_nus_job') OR
                (trackingCode == 'rj_nus') OR
                (trackingCode == 'rj_ad_apply') OR
                (trackingCode == 'rj_ad_title') OR
                (trackingCode == 'rj_jss_lp') OR
                (trackingCode == 'fjr_results') OR
                (trackingCode == 'sjymbii_je') OR
                (trackingCode == 'sjymbii_jhp') OR
                (trackingCode == 'sjymbii_hpw') OR
                (trackingCode == 'sjymbii_jjsp') OR
                (trackingCode == 'jymbii_jobs_name');

viewpair = foreach job_applications_j4u generate memberId, jobId, time; 

-- positive pair that users apply and come from jymbii recommendation 
pospair = join applypair by (memberId, jobId), viewpair by (memberId, jobId) parallel 2000; 
pospair = foreach pospair generate applypair::memberId as memberId, applypair::jobId as jobId, applypair::time as applytime, viewpair::time as viewtime;

-- score = load '$OUTPUT/JYMBII-batch/history/member-job-score/$REPORT_DATE' USING BinaryJSON();
-- score = foreach score generate sourceId, FLATTEN(hits);
-- score = foreach score generate sourceId as memberId, hits::id as jobId, hits::score as score; 
-- pospair = join score by (memberId, jobId), pospair by (memberId, jobId); 
-- pospair = foreach pospair generate pospair::memberId as memberId, pospair::jobId as jobId, score::score as score;  
pospair = distinct pospair parallel 1;

store pospair into '$OUTPUT/JYMBII-batch/history/positive/$REPORT_DATE' USING BinaryJSON('memberId'); 
