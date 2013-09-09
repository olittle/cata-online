---.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
--
-- File Name : TestData.pig
--
-- Purpose : Generate the Test Data 
--
-- Creation Date : 28-07-2013
--
-- Last Modified : Fri 09 Aug 2013 01:34:42 PM PDT
--
-- Created By : Huan Gui (hgui@linkedin.com) 
--
--_._._._._._._._._._._._._._._._._._._._._.

-- input data is the score of tmr 
-- %declare kernel   `uname -s`
-- %default now      `date "+%s"`
-- %default daysago  1
-- %declare tzoffset `bash -c 'if [[ "$kernel" == "Linux" ]]; then echo "-28800";  else echo "0"; fi'`
-- %declare tzero    `echo $(($now  + $tzoffset))`
-- 
-- %declare REPORT_DATE `bash -c 'if [[ "$kernel" == "Linux" ]]; then date -d "@$tzero" "+%Y/%m/%d"; else date -r $tzero "+%Y/%m/%d"; fi'`
-- %declare e `echo $REPORT_DATE`
-- 
-- mem = load '$OUTPUT/JYMBII-batch/TMP/MemList' USING BinaryJSON(); 
-- 
-- data = load '$OUTPUT/JYMBII-batch/history/recent-score/$REPORT_DATE' USING BinaryJSON();
-- data = foreach data generate sourceId as memberId, FLATTEN(hits);
-- data = foreach data generate memberId, hits::id as jobId, hits::score as score; 
-- 
-- data = join data by memberId, mem by memberId;
-- data = distinct data parallel 1;
-- data = foreach data generate 'p' as label, data::memberId as memberId, data::jobId as jobId, data::score as score; 
--  
-- store data into '$OUTPUT/JYMBII-batch/TMP/test' USING BinaryJSON('memberId'); 

%declare kernel   `uname -s`
%default now      `date "+%s"`
%declare tzoffset `bash -c 'if [[ "$kernel" == "Linux" ]]; then echo "-28800";  else echo "0"; fi'`
%declare tzero    `echo $(($now  + $tzoffset))`

%declare REPORT_DATE `bash -c 'if [[ "$kernel" == "Linux" ]]; then date -d "@$tzero" "+%Y/%m/%d"; else date -r $tzero "+%Y/%m/%d"; fi'`
%declare e `echo $REPORT_DATE`

RMF $OUTPUT/JYMBII-batch/TMP/test
RMF $OUTPUT/JYMBII-batch/TMP/orginal-score

mem = load '$OUTPUT/JYMBII-batch/TMP/MemList' USING BinaryJSON(); 

-- data_1 = load '$OUTPUT/JYMBII-batch/history/recent-score/$REPORT_DATE' USING BinaryJSON();
-- data = join data_1 by sourceId, mem by memberId USING 'replicated';
-- data = foreach data generate data_1::sourceId as memberId, data_1::hits as hits; 
-- data = foreach data generate memberId, FLATTEN(hits);
-- data = foreach data generate 'p' as label, memberId, hits::id as jobId, hits::score as score; 
-- data = distinct data parallel 1;
-- store data into '$OUTPUT/JYMBII-batch/TMP/test' USING BinaryJSON('memberId');
-- 
-- data_1 = load '$OUTPUT/JYMBII-batch/history/recent-score/$REPORT_DATE' USING BinaryJSON();
-- org = join data_1 by sourceId left outer, mem by memberId USING 'replicated'; 
-- org = foreach org generate 18041 as algorithmId, data_1::computeTime as computeTime, data_1::hits as hits, data_1::intent as intent, data_1::lastMod as lastMod, data_1::returnStatus as returnStatus, data_1::sourceId as sourceId, mem::memberId as check; 
-- org = filter org by check is null; 
-- org = foreach org generate algorithmId, computeTime, hits, intent, lastMod, returnStatus, sourceId;
-- org = distinct org parallel 10;
-- store org into '$OUTPUT/JYMBII-batch/TMP/orginal-score' USING BinaryJSON('intent', 'algorithmId', 'sourceId');

data_1 = load '$OUTPUT/JYMBII-batch/history/recent-score/$REPORT_DATE' USING BinaryJSON();
data = join data_1 by sourceId left outer, mem by memberId USING 'replicated';
data = foreach data generate data_1::algorithmId as algorithmId, data_1::computeTime as computeTime, data_1::hits as hits, data_1::intent as intent, data_1::lastMod as lastMod, data_1::returnStatus as returnStatus, data_1::sourceId as sourceId, mem::memberId as check;
data = filter data by algorithmId > 0; 
SPLIT data INTO active IF check is not null, inactive IF check is null;
 
data = foreach active generate sourceId as memberId, hits; 
data = foreach data generate memberId, FLATTEN(hits);
data = foreach data generate 'p' as label, memberId, hits::id as jobId, hits::score as score; 
data = distinct data parallel 1;
store data into '$OUTPUT/JYMBII-batch/TMP/test' USING BinaryJSON('memberId');

org = foreach inactive generate algorithmId, computeTime, hits, intent, lastMod, returnStatus, sourceId;
org = distinct org parallel 10;
store org into '$OUTPUT/JYMBII-batch/TMP/orginal-score' USING BinaryJSON('intent', 'algorithmId', 'sourceId');
