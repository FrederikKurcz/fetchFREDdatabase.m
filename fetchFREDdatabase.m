function[data] = fetchFREDdatabase(excelFileName,obsStart)

% Purpose: Download any number of FRED variables of possibly different
% frequencies and merge all variables of the same frequency in timetables.
% Automatically aggregate to lower frequency, if needed.
%
% This function reads an excel file called 'excelFileName' and looks for
% sheets called 'daily','weekly','monthly','quarterly','annual'. The excel
% cells contain the FRED variable IDs, one ID in one cell.
% If the user wishes a series to be aggregated to a lower frequency, write 
% the series ID in the sheet with the desired frequency. E.g., if 
% PAYEMS (monthly) is needed in quarterly frequency, write PAYEMS in the 
% excel sheet with the name 'quarterly' (method of aggregation: mean).
%
% Inputs:
% - excelFileName: string of the name of the excel file (include the file
%   type, e.g. .xlsx)
% - obsStart: A cell array containing a string of the starting date in the 
%   format: 'yyyy-MM-dd'. It is possible to supply a different starting date
%   for each frequency, by supplying a cell array with rows equal to the
%   number of different frequencies needed. 
%
% Output:
% - data: a structure containing one timetable and one metadata table for
%   each frequency of variables downloaded. E.g., if 5 quarterly and 5 monthly
%   data series were downloaded, there would be two timetables, one for 
%   quarterly and one for monthly data series, and two tables containing
%   metadata.
%
% Notes:
% - Matlab's Datafeed Toolbox is required.
% - Data is downloaded from obsStart until 4 years hence from today, e.g. 
%   for FOMC forecasts
% - Weekly data is realigned to "occur" on Fridays, irrespective of the
%   original day of the week. This standardization is important for merging.
% - Data is aggregated using the mean.
% - At the end of the sample, data is only aggregated to lower frequency if
%   all observations to fill the lower frequency unit are available. E.g.,
%   if only two months of the last quarter are available, those last two 
%   months will be dropped (this is not the case at the beginning of the
%   sample, i.e. whatever is available will be aggregated).
% - Variables which are aggregated to lower frequency are first linearly
%   interpolated for missing values (otherwise the retime() command doesn't
%   aggregate). Data is not extrapolated.
% - Convention used: quarterly data "occur" on the first day of the third
%   month of the quarter.
% - Convention used for annual data: "occur" on the first day of July.
%
% Frederik Kurcz
% 10.12.2021
% 
% Contact: frederik.kurcz@gmail.com

%% 1. Check environment and inputs

% possible frequencies:
frequencies = {'daily','weekly','monthly','quarterly','annual'};

% check whether the datafeed toolbox is installed and licensed:
if ~license('test','Datafeed_Toolbox')
    error('The datafeed toolbox is not licensed, which is a required toolbox for this function.')
end

% check for which frequencies data is to be downloaded:
[~,SHEETS] = xlsfinfo(excelFileName);
required_freq_index = find(ismember(frequencies,lower(SHEETS)));

% check the excel file is set up correctly:
if isempty(required_freq_index)
    error(['The excel file ',excelFileName,' contains no sheets called ',... 
           'either daily, weekly, monthly, quarterly, or annual.'])
end

% check that the starting date is supplied correctly:
if ~iscell(obsStart)
    error('The input obsStart needs to be a cell.')
else
    obsStart = obsStart(:);
    % If only one starting date for data is given, use this starting date
    % for all frequencies:
    while length(obsStart) < length(required_freq_index)
        obsStart = [obsStart; obsStart(end)];
    end
end

% If there is annual data, modify the starting date to be 1.st Jan of the
% given year. Reason: function retime that aggregates data uses 1.st Jan
% for annual data. If I aggregate data and merge with the empty timetable
% created below, and the empty timetable does not use the 1st. Jan as the 
% day of the year, the merging of the annual data won't work.
if any(required_freq_index == length(frequencies))
    obsStart{end} = [obsStart{end}(1:4),'-01-01'];
end

data_freq = {'days','weeks','months','quarters','years'};  % for caldays,... command

obsEnd = datestr(datetime('today')+calyears(4),'yyyy-mm-dd');   % data is downloaded 4 years hence, e.g. for FOMC forecasts

%% 2. Initialize tables and connection

% Create empty timetables except for the dates: (this makes it possible to
% simply use outerjoin() later)
for ii = 1:length(required_freq_index)
    data.(frequencies{required_freq_index(ii)}) = ...
        timetable(((datetime(obsStart{ii}):eval(['cal',data_freq{required_freq_index(ii)}]):datetime(obsEnd))'));
    data.([frequencies{required_freq_index(ii)},'_meta']) = table();
end

% Establish connection to FRED database
try
c = fred('https://fred.stlouisfed.org/');
catch
    pause(10)   % sometimes if establishing the connection takes longer, it stops early, so try again
    c = fred('https://fred.stlouisfed.org/');
end
c.DataReturnFormat = 'table';
c.DatetimeType = 'datetime';

%% 3. Download data and merge into datasets

% Loop over frequencies
for freq = frequencies(required_freq_index)
    
    % convert cell to character string (matlab doesn't like changing the
    % loop index variable's type)
    freq = char(freq);%#ok
    
    % read in series names from the excel sheet:
    [~,SN.(freq)] = xlsread(excelFileName,freq);

    % only keep unique, non-empty cells
    SN.(freq) = unique((SN.(freq)(~cellfun(@isempty,SN.(freq))))','stable'); 
    
    % loop over variables of one frequency
    for ii = 1:length(SN.(freq))

        try
            % fetch data
            out = fetch(c,SN.(freq){ii},obsStart{strcmpi(freq,frequencies(required_freq_index))},obsEnd);
            
            % original frequency of the data series:
            SN_freq = lower(strtrim(out.Frequency{1}));
            if contains(SN_freq,',')
                SN_freq = extractBefore(SN_freq,',');
            end
            
            % Convert all weekly data to ending on Fridays 
            if strcmpi('weekly',freq)
                out = ReallignWeeklyDataEndingFriday(out);
            end

            % aggregate data if it's of higher frequency
            if ~strcmpi(freq,SN_freq) 
                                
                % retime only accepts 'yearly'... therefore:
                if strcmpi(freq,'annual'); freq_retime = 'yearly';else; freq_retime = freq; end
                
                % append exactly one time unit with NaN as the observation
                % at the end of the sample to prevent aggregation of the 
                % last possibly incomplete time unit. Fill with linearly 
                % interpolated values but for the end of the sample, then aggregate
                dataAggregated = retime(fillmissing(timetable([out.Data{1}.Var1;out.Data{1}.Var1(end)+...
                                        eval(['cal',data_freq{strcmpi(frequencies,SN_freq)}])],...
                                        [out.Data{1}.Var2; NaN]),'linear','EndValues','none'),freq_retime,@mean);
                % merge data
                data.(freq) = outerjoin(data.(freq),dataAggregated);
                
            elseif strcmpi(freq,SN_freq)
                
                % merge data
                data.(freq) = outerjoin(data.(freq),timetable(out.Data{1}.Var1,out.Data{1}.Var2));
                
            end
            
            % keep metadata in a separate table
            data.([freq,'_meta'])(:,strtrim(out.SeriesID)) = table(strtrim([out.Title;...
                lower(out.Units);out.SeasonalAdjustment;out.Source]),'Rownames', {'Title', 'Units', 'SA', 'Source'});
            
        catch
            disp(['Something went wrong when downloading series ',SN.(freq){ii},'.'])
            break
        end
        
        % print progress to the command window
        disp(['FRED, ',freq,': ',num2str(ii), ' of ',num2str(length(SN.(freq))),' downloaded.'])
        
    end

    % Assign variable names:
    data.(freq).Properties.VariableNames = data.([freq,'_meta']).Properties.VariableNames;
    
    % Move quarterly data date to third month of the quarter
    if strcmpi(freq,'quarterly')
        data.(freq).Properties.RowTimes = data.(freq).Properties.RowTimes+calmonths(2);
    end
    
    % move annual data to first day of July; by default it's first Jan
    if strcmpi(freq,'annual')
        data.(freq).Properties.RowTimes = data.(freq).Properties.RowTimes+calmonths(6);
    end
    
    % remove rows with all missing data
    data.(freq) = rmmissing(data.(freq),1,'MinNumMissing',size(data.(freq){:,:},2));
    
end

% Help function to align weekly data:
function[out]=ReallignWeeklyDataEndingFriday(in)
% find the weekday and adjust such that all weekly data is on friday (for
% merging data series correctly)

[~,day] = weekday(in.Data{1,1}.Var1);

switch day(1,:)
    case 'Mon'
        AdjFriday = -3;
    case 'Tue'
        AdjFriday = -4;
    case 'Wed'
        AdjFriday = 2;
    case 'Thu'
        AdjFriday = 1;
    case 'Fri'
        AdjFriday = 0;
    case 'Sat'
        AdjFriday = -1;
    case 'Sun'
        AdjFriday = -2;
end

in.Data{1,1}.Var1 = in.Data{1,1}.Var1 + caldays(AdjFriday);
out = in;
