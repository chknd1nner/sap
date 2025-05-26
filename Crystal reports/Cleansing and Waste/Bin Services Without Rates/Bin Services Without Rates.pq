// Server
"proddbinfomart\apps02" meta [IsParameterQuery=true, Type="Any", IsParameterQueryRequired=true]

// Database
"Infoprod" meta [IsParameterQuery=true, Type="Any", IsParameterQueryRequired=true]

// Property
let
    Source = Sql.Database(Server, Database),
    infodbo_Property = Source{[Schema="infodbo",Item="Property"]}[Data],
    #"Filtered Rows" = Table.SelectRows(infodbo_Property, each [Property_Type] <> "STRCARSPC " and [Property_Type] <> "UTILITY   " and [Status] = "C"),
    #"Removed Other Columns" = Table.SelectColumns(#"Filtered Rows",{"Property_key", "Formatted_Address", "Address", "Suburb", "Street", "Status", "TPKLPAPROP", "Parent_Property_Key", "Parent_Property_Address", "Formatted_Owner_Name", "Owners_NA_slab_1", "Owners_NA_slab_2", "Owners_NA_slab_3", "Owners_NA_slab_4", "Owners_NA_slab_5", "Assessment", "Assessment_key"})
in
    #"Removed Other Columns"

// Property_Services_Facts
// Property_Services_Facts
let
    Source = Sql.Database(Server, Database),
    infodbo_Property_Services_Facts = Source{[Schema="infodbo",Item="Property_Services_Facts"]}[Data]
in
    infodbo_Property_Services_Facts

// fnCalendar
let
    Source = (StartDate as date, EndDate as date) as table =>
let
    Duration = Duration.Days( EndDate - StartDate ) + 1,
    Increment = #duration(1,0,0,0),
    DateList = List.Dates(StartDate,Duration,Increment),
    //The calendar columns as individual lists
    ColList = {
        List.Combine({{"Date"},DateList})
        //Calendar year, quarter, month
        ,List.Combine({{"Year"}, List.Transform(DateList,each Date.Year(_) )})
        ,List.Combine({{"Quarter"}, List.Transform(DateList,each Date.QuarterOfYear(_) )})
        ,List.Combine({{"Month"}, List.Transform(DateList,each Date.Month(_) )})
        ,List.Combine({{"Month of quarter"}, List.Transform(DateList,each Date.Month(_) - ((Date.QuarterOfYear(_) - 1) * 3))})
        //Week of year/month
        ,List.Combine({{"Week of year"}, List.Transform(DateList,each Date.WeekOfYear(_,Day.Sunday))})
        ,List.Combine({{"Week of month"}, List.Transform(DateList,each Date.WeekOfMonth(_,Day.Sunday))})
        //Day of year/month/week
        ,List.Combine({{"Day of year"}, List.Transform(DateList,each Date.DayOfYear(_) )})
        ,List.Combine({{"Day of month"}, List.Transform(DateList,each Date.Day(_) )})
        ,List.Combine({{"Day of week"}, List.Transform(DateList,each Date.DayOfWeek(_,Day.Sunday) + 1 )})
        //Fiscal year, quarter, month
        ,List.Combine({{"Financial year"}, List.Transform(DateList,each if Date.Month(_) > 6 then Date.Year(_)+1 else Date.Year(_) )})
        ,List.Combine({{"Financial quarter"}, List.Transform(DateList,each if Date.QuarterOfYear(_) < 3 then Date.QuarterOfYear(_) + 2 else Date.QuarterOfYear(_) - 2 )})
        ,List.Combine({{"Financial month"}, List.Transform(DateList,each if Date.Month(_) > 6 then Date.Month(_) - 6 else Date.Month(_) + 6 )})
        //Month name long and short, day name
        ,List.Combine({{"Month name"}, List.Transform(DateList,each Date.MonthName(_) )})
        ,List.Combine({{"Short month name"}, List.Transform(DateList,each Date.ToText(_, "MMM") )})
        ,List.Combine({{"Day name"}, List.Transform(DateList,each Date.DayOfWeekName(_) )})
        ,List.Combine({{"Day name short"}, List.Transform(DateList,each Text.Start(Date.DayOfWeekName(_),3) )})
        ,List.Combine({{"Day name shorter"}, List.Transform(DateList,each Text.Start(Date.DayOfWeekName(_),1) )})
        //First days of
        ,List.Combine({{"First day of quarter"}, List.Transform(DateList,each Date.StartOfQuarter(_) )})
        ,List.Combine({{"First day of month"}, List.Transform(DateList,each Date.StartOfMonth(_) )})
        ,List.Combine({{"First day of week"}, List.Transform(DateList,each Date.StartOfWeek(_,Day.Sunday) )})
        //Flags
        ,List.Combine({{"Is weekday?"}, List.Transform(DateList,each if List.Contains({2..6},Date.DayOfWeek(_,Day.Sunday)+1 ) then 1 else 0 )})
        //Labels
        ,List.Combine({{"Quarter label"}, List.Transform(DateList,each "Q" & Text.From(Date.QuarterOfYear(_)) )})
        ,List.Combine({{"Financial quarter label"}, List.Transform(DateList,each "FQ" & Text.From(if Date.QuarterOfYear(_) < 3 then Date.QuarterOfYear(_) + 2 else Date.QuarterOfYear(_) - 2 ))})
    },
    //Combine the lists into a table and promote the header row
    ListsCombined = Table.PromoteHeaders(Table.FromColumns(ColList)),
    //Detect and transform the data type of each column based on the first row (Source{0})
    CalendarTable = Table.TransformColumnTypes(ListsCombined, List.Transform(Table.ColumnNames(ListsCombined), each {_, if Record.Field(ListsCombined{0},_) = null then Any.Type else Value.Type(Record.Field(ListsCombined{0},_))}))
in
    CalendarTable
in
    Source

// Mindate
let
    Source = Sql.Database(Server, Database),
    infodbo_Reg_BSERV_Bin_Services = Source{[Schema="infodbo",Item="Reg_BSERV_Bin_Services"]}[Data],
    C_AUDIT_DATE1 = infodbo_Reg_BSERV_Bin_Services[C_AUDIT_DATE],
    #"Converted to Table" = Table.FromList(C_AUDIT_DATE1, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Changed Type" = Table.TransformColumnTypes(#"Converted to Table",{{"Column1", type date}}),
    #"Calculated Earliest" = List.Min(#"Changed Type"[Column1])
in
    #"Calculated Earliest"

// Maxdate
let
    Source = Sql.Database(Server, Database),
    infodbo_Reg_BSERV_Bin_Services = Source{[Schema="infodbo",Item="Reg_BSERV_Bin_Services"]}[Data],
    C_AUDIT_DATE1 = infodbo_Reg_BSERV_Bin_Services[C_AUDIT_DATE],
    #"Converted to Table" = Table.FromList(C_AUDIT_DATE1, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    #"Changed Type" = Table.TransformColumnTypes(#"Converted to Table",{{"Column1", type date}}),
    #"Calculated Earliest" = List.Max(#"Changed Type"[Column1])
in
    #"Calculated Earliest"

// Calendar
let
    Source = fnCalendar(#date(Date.Year(Mindate), 1, 1), #date(Date.Year(Maxdate), 12, 31)),
    #"Add FY label" = Table.AddColumn(Source, "FY label", each 
if 
    [Financial year] - Date.Year(Date.AddMonths(UTCToAEST(DateTime.LocalNow()),6)) = 0 
then 
    "Current FY" 
else
    /*if
        [Financial year] - Date.Year(Date.AddMonths(UTCToAEST(DateTime.LocalNow()),6)) = 1
    then
        "Next FY"
    else*/
        Text.From([Financial year] - 1) & "-" & Text.From([Financial year]), Text.Type),
    #"Add CY label" = Table.AddColumn(#"Add FY label", "CY label", each if [Year] - Date.Year(UTCToAEST(DateTime.LocalNow())) = 0 then "Current year" else Text.From([Year]), Text.Type),
    #"Add relative CY" = Table.AddColumn(#"Add CY label", "Relative CY", each  _[Year] - Date.Year(UTCToAEST(DateTime.LocalNow())), Int64.Type),
    #"Add relative FY" = Table.AddColumn(#"Add relative CY", "Relative FY",  each [Financial year] - Date.Year(Date.AddMonths(UTCToAEST(DateTime.LocalNow()),6)), Int64.Type)
in
    #"Add relative FY"

// UTCToAEST
let
    UTCToAEST = (DateTimeToConvert as datetime) =>
let
    //Convert the input to AEST (UTC+10)
    AESTDateTime = DateTimeToConvert + #duration(0, 10, 0, 0),
    Timezone = DateTimeZone.ZoneHours(DateTimeZone.From(DateTimeToConvert)),

    DLSStart = 
        if AESTDateTime >= #datetime(2008,10,5,2,0,0) //2008 onwards first Sunday in Oct 
            then DateTime.From(Date.StartOfWeek(#date(Date.Year(AESTDateTime), 10, 7), Day.Sunday)) + #duration(0,2,0,0)
        else if AESTDateTime >= #datetime(1971,10,31,2,0,0) //1971 - 2008 last Sunday in Oct 
            then DateTime.From(Date.EndOfMonth((#date(Date.Year(AESTDateTime), 10, 1)), Day.Monday)) + #duration(0,2,0,0) 
        else null,

    DLSEnd = 
        if AESTDateTime >= #datetime(2008,10,5,2,0,0) //2008 onwards first Sunday in Apr 
            then DateTime.From(Date.StartOfWeek(#date(Date.Year(AESTDateTime), 4, 7), Day.Sunday)) + #duration(0,2,0,0)
        else if AESTDateTime >= #datetime(1971,10,31,2,0,0) //1971 - 2008 last Sunday in Mar 
            then DateTime.From(Date.EndOfMonth((#date(Date.Year(AESTDateTime), 3, 1)), Day.Monday)) + #duration(0,2,0,0) 
        else null,

    //Returns true (1) if we are in DST, otherwise false (0).
    isSummerTime = AESTDateTime >= DLSStart or AESTDateTime < DLSEnd,

    //Returns one (1) if we are in DST, otherwise zero (0) 
    DSTOffset = (Number.From(isSummerTime)),

    Output = AESTDateTime + #duration(0, DSTOffset, 0, 0)
in   
    Output
in
    UTCToAEST

// Property_Service_Codes_Custom
// Property_Service_Codes_Custom
let
    Source = Sql.Database(Server, Database),
    infodbo_Property_Service_Codes_Custom = Source{[Schema="infodbo",Item="Property_Service_Codes_Custom"]}[Data]
in
    infodbo_Property_Service_Codes_Custom

// Reg_BSERV_Bin_Services
// Reg_BSERV_Bin_Services
let
    Source = Sql.Database(Server, Database),
    infodbo_Reg_BSERV_Bin_Services = Source{[Schema="infodbo",Item="Reg_BSERV_Bin_Services"]}[Data]
in
    infodbo_Reg_BSERV_Bin_Services

// Rate_Assessment
// Rate_Assessment
let
    Source = Sql.Database(Server, Database),
    infodbo_Rate_Assessment = Source{[Schema="infodbo",Item="Rate_Assessment"]}[Data]
in
    infodbo_Rate_Assessment

// Rate_Type_Facts
// Rate_Type_Facts
let
    Source = Sql.Database(Server, Database),
    infodbo_Rate_Type_Facts = Source{[Schema="infodbo",Item="Rate_Type_Facts"]}[Data]
in
    infodbo_Rate_Type_Facts

// Rate_Type
// Rate_Type
let
    Source = Sql.Database(Server, Database),
    infodbo_Rate_Type = Source{[Schema="infodbo",Item="Rate_Type"]}[Data]
in
    infodbo_Rate_Type

// CurrentRateTypes
// CurrentRateTypes
let
    Source = #"Rate_Type",
    FilteredRows = Table.SelectRows(Source, each ([IS_Current_Period] = "Y") and ([Category_Code] = "DWM")),
    AddedBinType = Table.AddColumn(FilteredRows, "rate_bin_type", each Text.Middle([Ratetype], 3, 3) ),
    AddedFrequency = Table.AddColumn(AddedBinType, "rate_frequency", each Text.End([Ratetype],2), Int16.Type)
in
    AddedFrequency

// AssessmentInfo
// AssessmentInfo
let
    Source = #"Rate_Assessment",
    SelectColumns = Table.SelectColumns(Source, {"TFKLPAPROP", "Assessment", "Vgnumber"})
in
    SelectColumns

// ParentProperties
// ParentProperties
let
    Source = #"Property",
    FilteredRows = Table.SelectRows(Source, each [Parent_Property_Key] = null)
in
    FilteredRows

// ChildProperties
// ChildProperties
let
    Source = #"Property", 
    FilteredRows = Table.SelectRows(Source, each [Parent_Property_Key] <> null)
in
    FilteredRows

// BinServicesInfo
// BinServicesInfo
let
    Source = #"Reg_BSERV_Bin_Services",
    SelectColumns = Table.SelectColumns(Source, {"Property_Key_LINK_TO_PROPERTY", "C_CALCULATION_METHOD"}),
    RenamedColumns = Table.RenameColumns(SelectColumns, {{"Property_Key_LINK_TO_PROPERTY", "Property_key"}}),
    AddedCalculationMethod = Table.AddColumn(RenamedColumns, "calculation_method", each 
        if [C_CALCULATION_METHOD] = null then "None" else Text.Start([C_CALCULATION_METHOD], 4), Text.Type),
    RemovedOrigColumn = Table.RemoveColumns(AddedCalculationMethod, {"C_CALCULATION_METHOD"})
in
    RemovedOrigColumn

// ChildRatesTotals
// ChildRatesTotals
let
    Source = #"ChildProperties",
    // Join rates for child properties
    JoinedRates = Table.NestedJoin(
        Source, {"Property_key"}, 
        #"PropertyRates", {"Property_key"}, 
        "Rates", JoinKind.LeftOuter),
    ExpandedRates = Table.ExpandTableColumn(
        JoinedRates, "Rates", 
        {"rate_units", "number_of_rates"}),
    
    // Replace nulls with zeros
    ReplacedNulls = Table.ReplaceValue(
        ExpandedRates, null, 0, Replacer.ReplaceValue, 
        {"rate_units", "number_of_rates"}),
    
    // Group by parent property for totals
    GroupedByParent = Table.Group(ReplacedNulls, {"Parent_Property_Key"}, {{"total_child_rate_units", each List.Sum([rate_units]), type nullable number}, {"total_child_rates", each List.Sum([number_of_rates]), type nullable number}, {"total_child_props", each Table.RowCount(_), Int64.Type}}),
    
    // Rename to match join field
    RenamedColumn = Table.RenameColumns(
        GroupedByParent, {{"Parent_Property_Key", "Property_key"}})
in
    RenamedColumn

// PropertyRates
// PropertyRates
let
    Source = #"Rate_Type_Facts",
    // Join rate types to get only current DWM rates
    JoinedRateTypes = Table.NestedJoin(
        Source, {"Ratetype_Key"}, 
        #"CurrentRateTypes", {"Ratetype_Key"}, 
        "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(
        JoinedRateTypes, "RateTypeDetails", 
        {"IS_Current_Period", "Category_Code"}),
    
    // Filter to active, unsuspended rates
    FilteredRows = Table.SelectRows(
        ExpandedRateTypes, each 
            [IS_Current_Period] = "Y" and 
            [Category_Code] = "DWM" and 
            ([Suspended_Date] = null or [Suspended_Date] > DateTime.LocalNow())),
    
    // Group by property for rate summaries
    GroupedByProperty = Table.Group(
        FilteredRows, {"Property_key"}, {
            {"number_of_rates", each Table.RowCount(_), Int64.Type},
            {"rate_units", each List.Sum([Units]), Int64.Type}
        })
in
    GroupedByProperty

// BinServicesWithoutRates
// BinServicesWithoutRates
let
    // Start with parent properties
    Source = #"ParentProperties",
    
    // Join assessment info for properties
    JoinedAssessment = Table.NestedJoin(
        Source, {"TPKLPAPROP"}, 
        #"AssessmentInfo", {"TFKLPAPROP"}, 
        "AssessmentInfo", JoinKind.LeftOuter),
    ExpandedAssessment = Table.ExpandTableColumn(
        JoinedAssessment, "AssessmentInfo", 
        {"Vgnumber"},{"Vgnumber"}),
    
    // Join property services 
    JoinedServices = Table.NestedJoin(
        ExpandedAssessment, {"Property_key"}, 
        #"PropertyServices", {"Property_key"}, 
        "Services", JoinKind.LeftOuter),
    ExpandedServices = Table.ExpandTableColumn(
        JoinedServices, "Services", 
        {"number_of_services", "number_of_red_services", "service_category"}, {"number_of_services", "number_of_red_services", "service_category"}),
    
    // Join bin services configuration
    JoinedBinServices = Table.NestedJoin(
        ExpandedServices, {"Property_key"}, 
        #"BinServicesInfo", {"Property_key"}, 
        "BinServices", JoinKind.LeftOuter),
    ExpandedBinServices = Table.ExpandTableColumn(
        JoinedBinServices, "BinServices", 
        {"calculation_method"}, {"calculation_method"}),
    
    // Join property rates information
    JoinedRates = Table.NestedJoin(
        ExpandedBinServices, {"Property_key"}, 
        #"PropertyRates", {"Property_key"}, 
        "Rates", JoinKind.LeftOuter),
    ExpandedRates = Table.ExpandTableColumn(
        JoinedRates, "Rates", 
        {"number_of_rates", "rate_units"}),
    
    // Join child properties data
    JoinedChildProps = Table.NestedJoin(
        ExpandedRates, {"Property_key"}, 
        #"ChildRatesTotals", {"Property_key"}, 
        "ChildRates", JoinKind.LeftOuter),
    ExpandedChildProps = Table.ExpandTableColumn(
        JoinedChildProps, "ChildRates", 
        {"total_child_rates", "total_child_rate_units", "total_child_props"}, {"total_child_rates", "total_child_rate_units", "total_child_props"}),
    
    // Replace nulls with zeros for numeric columns
    ReplacedNulls = Table.ReplaceValue(
        ExpandedChildProps, null, 0, Replacer.ReplaceValue, 
        {"number_of_services", "number_of_red_services", "number_of_rates", 
         "rate_units", "total_child_rates", "total_child_rate_units", "total_child_props"}),
    
    // Final filter for properties with services but no rates
    FilteredRows = Table.SelectRows(ReplacedNulls, each 
        [number_of_services] > 0 and
        [total_child_rate_units] = 0 and
        [number_of_rates] = 0),
    #"Trimmed Text" = Table.TransformColumns(FilteredRows,{{"Formatted_Owner_Name", Text.TrimEnd, type text}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Trimmed Text",{{"Assessment", type text}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{{"Formatted_Address", "Formatted address"}, {"TPKLPAPROP", "TPK"}, {"Formatted_Owner_Name", "Owner(s)"}, {"service_category", "Service category"}, {"calculation_method", "Calculation method"}})
in
    #"Renamed Columns"

// PropertyServices
// PropertyServices
let
    Source = #"Property_Services_Facts",
    // Filter to active services of interest
    FilteredActiveServices = Table.SelectRows(Source, each 
        ([Status] = "C" or [Removal_Date] > DateTime.LocalNow()) and 
        (Text.Start([Service_Code], 1) = "R" or 
         Text.Start([Service_Code], 1) = "G" or 
         Text.Start([Service_Code], 1) = "N")),
    
    // Join service code info
    JoinedServiceCodes = Table.NestedJoin(FilteredActiveServices, {"Service_Code"}, 
        #"Property_Service_Codes_Custom", {"Service_Code"}, 
        "ServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedServiceCodes = Table.ExpandTableColumn(JoinedServiceCodes, 
        "ServiceCodeInfo", {"Is_Active"}, {"Is_Active"}),
    
    // Apply bin service specific filter  
    FilteredBinServices = Table.SelectRows(ExpandedServiceCodes, each 
        Text.Start([Service_Code], 1) = "R" or 
        (Text.Start([Service_Code], 1) = "G" and [Is_Active] = "No") or 
        Text.Start([Service_Code], 1) = "N"),
    
    // Add dummy column for red services calculation that will fold to SQL
    AddedRedServiceCount = Table.AddColumn(FilteredBinServices, "red_service_count", each
        if Text.Start([Service_Code], 1) = "R" then [Number_of_Services] else 0, Int64.Type),
        
    // Calculate service metrics by property
    GroupedByProperty = Table.Group(AddedRedServiceCount, {"Property_key"}, {
        {"number_of_services", each List.Sum([Number_of_Services]), Int64.Type},
        {"number_of_red_services", each List.Sum([red_service_count]), Int64.Type},
        {"service_codes_list", each List.Min([Service_Code]), Text.Type},
        {"service_active_list", each List.Min([Is_Active]), Text.Type}
    }),
    AddedServiceCategory = Table.AddColumn(GroupedByProperty, "service_category", each if [service_active_list] = null then "Missing service" else if [number_of_services] = 0 then "Zero service" else if [service_active_list] = "Yes" then "Has service" else if [service_active_list] = "No" then "Old Service Code" else "Unknown", Text.Type),
    
    // Clean up temporary columns
    FinalTable = Table.RemoveColumns(AddedServiceCategory, 
        {"service_codes_list", "service_active_list"})
in
    FinalTable

// SUDProperties
// SUDProperties  
let
    Source = ParentProperties,
    // Left join to find properties that have children
    JoinedChildren = Table.NestedJoin(
        Source, {"Property_key"}, 
        Property, {"Parent_Property_Key"}, 
        "Children", JoinKind.LeftOuter),
    ExpandedChildren = Table.ExpandTableColumn(
        JoinedChildren, "Children", 
        {"Property_key"}, {"Child_Property_key"}),
    // Filter to properties with no children (SUDs)
    FilteredSUDs = Table.SelectRows(ExpandedChildren, each [Child_Property_key] = null),
    RemovedChildColumn = Table.RemoveColumns(FilteredSUDs, {"Child_Property_key"})
in
    RemovedChildColumn

// SUDServices
// SUDServices
let
    Source = Property_Services_Facts,
    // Filter to active services of interest (same logic as original)
    FilteredActiveServices = Table.SelectRows(Source, each 
        ([Status] = "C" or [Removal_Date] > DateTime.LocalNow()) and 
        (Text.Start([Service_Code], 1) = "R" or 
         Text.Start([Service_Code], 1) = "G" or 
         Text.Start([Service_Code], 1) = "N")),
    
    // Join service code info for G service filtering
    JoinedServiceCodes = Table.NestedJoin(FilteredActiveServices, {"Service_Code"}, 
        Property_Service_Codes_Custom, {"Service_Code"}, 
        "ServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedServiceCodes = Table.ExpandTableColumn(JoinedServiceCodes, 
        "ServiceCodeInfo", {"Is_Active"}, {"Is_Active"}),
    
    // Apply bin service specific filter (R services, inactive G services, N services)
    FilteredBinServices = Table.SelectRows(ExpandedServiceCodes, each 
        Text.Start([Service_Code], 1) = "R" or 
        (Text.Start([Service_Code], 1) = "G" and [Is_Active] = "No") or 
        Text.Start([Service_Code], 1) = "N"),
    
    // Exclude N0000UA from service counts
    FilteredExcludingUA = Table.SelectRows(FilteredBinServices, each [Service_Code] <> "N0000UA"),
    
    // Add dummy columns for aggregation that will fold to SQL
    AddedRedServiceCount = Table.AddColumn(FilteredExcludingUA, "red_service_count", each
        if Text.Start([Service_Code], 1) = "R" then [Number_of_Services] else 0, Int64.Type),
    
    // Group by property for service summaries
    GroupedByProperty = Table.Group(AddedRedServiceCount, {"Property_key"}, {
        {"number_of_services", each List.Sum([Number_of_Services]), Int64.Type},
        {"number_of_red_services", each List.Sum([red_service_count]), Int64.Type},
        {"service_active_list", each List.Min([Is_Active]), Text.Type}
    }),
    
    // Add service category
    AddedServiceCategory = Table.AddColumn(GroupedByProperty, "service_category", each 
        if [service_active_list] = null then "Missing Service"
        else if [number_of_services] = 0 then "Zero Service" 
        else if [service_active_list] = "Yes" then "Has Service"
        else if [service_active_list] = "No" then "Old Service Code"
        else "Unknown", Text.Type),
    
    RemovedTempColumn = Table.RemoveColumns(AddedServiceCategory, {"service_active_list"})
in
    RemovedTempColumn

// SUDRates
// SUDRates
let
    Source = Rate_Type_Facts,
    // Join current DWM rate types
    JoinedRateTypes = Table.NestedJoin(
        Source, {"Ratetype_Key"}, 
        Rate_Type, {"Ratetype_Key"}, 
        "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(
        JoinedRateTypes, "RateTypeDetails", 
        {"IS_Current_Period", "Category_Code", "Ratetype"}),
    
    // Filter to current DWM rates, exclude N0000UA
    FilteredRates = Table.SelectRows(ExpandedRateTypes, each 
        [IS_Current_Period] = "Y" and 
        [Category_Code] = "DWM" and 
        [Ratetype] <> "N0000UA" and
        ([Suspended_Date] = null or [Suspended_Date] > DateTime.LocalNow())),
    
    // Join assessment info
    JoinedAssessment = Table.NestedJoin(
        FilteredRates, {"Assessment_Key"}, 
        Rate_Assessment, {"Assessment_Key"}, 
        "AssessmentInfo", JoinKind.Inner),
    ExpandedAssessment = Table.ExpandTableColumn(
        JoinedAssessment, "AssessmentInfo", 
        {"Assessment", "Vgnumber", "Ratepayer_Address_1"}, 
        {"Assessment", "Vgnumber", "RatePayer"}),
    
    SelectedColumns = Table.SelectColumns(ExpandedAssessment, 
        {"Property_key", "Ratetype", "Assessment", "Vgnumber", "RatePayer"})
in
    SelectedColumns

// MUDParentProperties
// MUDParentProperties
let
    Source = ParentProperties,
    // Inner join with child properties to find parents that actually have children
    JoinedChildren = Table.NestedJoin(
        Source, {"Property_key"}, 
        ChildProperties, {"Parent_Property_Key"}, 
        "Children", JoinKind.Inner),
    ExpandedChildren = Table.ExpandTableColumn(
        JoinedChildren, "Children", 
        {"Property_key"}, {"Child_Property_key"}),
    // Group by parent to get distinct parent properties (since one parent can have multiple children)
    GroupedByParent = Table.Group(ExpandedChildren, 
        {"Property_key", "TPKLPAPROP", "Formatted_Address", "Suburb", "Status"}, {})
in
    GroupedByParent

// ParentChildServiceAggregation
// ParentChildServiceAggregation
let
    Source = MUDParentProperties,
    
    // Join all children for each parent
    JoinedChildren = Table.NestedJoin(
        Source, {"Property_key"}, 
        ChildProperties, {"Parent_Property_Key"}, 
        "Children", JoinKind.LeftOuter),
    ExpandedChildren = Table.ExpandTableColumn(
        JoinedChildren, "Children", 
        {"Property_key", "TPKLPAPROP", "Formatted_Address", "Suburb"}, 
        {"Child_Property_key", "Child_TPKLPAPROP", "Child_Formatted_Address", "Child_Suburb"}),
    
    // Join parent services
    JoinedParentServices = Table.NestedJoin(
        ExpandedChildren, {"Property_key"}, 
        Property_Services_Facts, {"Property_key"}, 
        "ParentServices", JoinKind.LeftOuter),
    ExpandedParentServices = Table.ExpandTableColumn(
        JoinedParentServices, "ParentServices", 
        {"Service_Code", "Number_of_Services", "Status", "Removal_Date"}, 
        {"Parent_Service_Code", "Parent_Number_of_Services", "Parent_Service_Status", "Parent_Removal_Date"}),
    
    // Join child services
    JoinedChildServices = Table.NestedJoin(
        ExpandedParentServices, {"Child_Property_key"}, 
        Property_Services_Facts, {"Property_key"}, 
        "ChildServices", JoinKind.LeftOuter),
    ExpandedChildServices = Table.ExpandTableColumn(
        JoinedChildServices, "ChildServices", 
        {"Service_Code", "Number_of_Services", "Status", "Removal_Date"}, 
        {"Child_Service_Code", "Child_Number_of_Services", "Child_Service_Status", "Child_Removal_Date"}),
    
    // Join parent service code info
    JoinedParentServiceCodes = Table.NestedJoin(
        ExpandedChildServices, {"Parent_Service_Code"}, 
        Property_Service_Codes_Custom, {"Service_Code"}, 
        "ParentServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedParentServiceCodes = Table.ExpandTableColumn(
        JoinedParentServiceCodes, "ParentServiceCodeInfo", 
        {"Is_Active"}, {"Parent_Is_Active"}),
    
    // Join child service code info  
    JoinedChildServiceCodes = Table.NestedJoin(
        ExpandedParentServiceCodes, {"Child_Service_Code"}, 
        Property_Service_Codes_Custom, {"Service_Code"}, 
        "ChildServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedChildServiceCodes = Table.ExpandTableColumn(
        JoinedChildServiceCodes, "ChildServiceCodeInfo", 
        {"Is_Active"}, {"Child_Is_Active"}),
    
    // Filter to relevant services and add calculation columns
    FilteredServices = Table.SelectRows(ExpandedChildServiceCodes, each 
        ([Parent_Service_Code] = null or 
         (([Parent_Service_Status] = "C" or [Parent_Removal_Date] > DateTime.LocalNow()) and
          (Text.Start([Parent_Service_Code], 1) = "R" or 
           (Text.Start([Parent_Service_Code], 1) = "G" and [Parent_Is_Active] = "No") or 
           Text.Start([Parent_Service_Code], 1) = "N"))) and
        ([Child_Service_Code] = null or 
         (([Child_Service_Status] = "C" or [Child_Removal_Date] > DateTime.LocalNow()) and
          (Text.Start([Child_Service_Code], 1) = "R" or 
           (Text.Start([Child_Service_Code], 1) = "G" and [Child_Is_Active] = "No") or 
           Text.Start([Child_Service_Code], 1) = "N")))),
    
    // Add dummy columns for aggregation
    AddedParentServiceCount = Table.AddColumn(FilteredServices, "parent_service_count", each
        if [Parent_Service_Code] <> null and [Parent_Service_Code] <> "N0000UA" 
        then [Parent_Number_of_Services] else 0, Int64.Type),
    AddedChildServiceCount = Table.AddColumn(AddedParentServiceCount, "child_service_count", each
        if [Child_Service_Code] <> null and [Child_Service_Code] <> "N0000UA" 
        then [Child_Number_of_Services] else 0, Int64.Type),
    AddedParentRedCount = Table.AddColumn(AddedChildServiceCount, "parent_red_count", each
        if [Parent_Service_Code] <> null and Text.Start([Parent_Service_Code], 1) = "R" 
        then [Parent_Number_of_Services] else 0, Int64.Type),
    AddedChildRedCount = Table.AddColumn(AddedParentRedCount, "child_red_count", each
        if [Child_Service_Code] <> null and Text.Start([Child_Service_Code], 1) = "R" 
        then [Child_Number_of_Services] else 0, Int64.Type),
    AddedChildAnyService = Table.AddColumn(AddedChildRedCount, "child_any_service", each
        if [Child_Service_Code] <> null then [Child_Number_of_Services] else 0, Int64.Type),
    AddedParentAnyService = Table.AddColumn(AddedChildAnyService, "parent_any_service", each
        if [Parent_Service_Code] <> null then [Parent_Number_of_Services] else 0, Int64.Type),
    AddedN0000UACount = Table.AddColumn(AddedParentAnyService, "n0000ua_count", each
        if ([Parent_Service_Code] = "N0000UA" and [Parent_Is_Active] = "Yes") or 
           ([Child_Service_Code] = "N0000UA" and [Child_Is_Active] = "Yes") 
        then 1 else 0, Int64.Type),
    
    // Group by parent property with aggregations
    GroupedByParent = Table.Group(AddedN0000UACount, 
        {"Property_key", "TPKLPAPROP", "Formatted_Address", "Suburb"}, {
        {"total_number_of_services", each List.Sum([parent_service_count]) + List.Sum([child_service_count]), Int64.Type},
        {"number_of_red_services", each List.Sum([parent_red_count]) + List.Sum([child_red_count]), Int64.Type},
        {"child_services_total", each List.Sum([child_any_service]), Int64.Type},
        {"parent_services_total", each List.Sum([parent_any_service]), Int64.Type},
        {"has_N0000UA_count", each List.Sum([n0000ua_count]), Int64.Type},
        {"parent_service_active", each List.Min([Parent_Is_Active]), Text.Type}
    }),
    
    // Add service category logic
    AddedServiceCategory = Table.AddColumn(GroupedByParent, "service_category", each
        if [child_services_total] > 0 then "Child Has Service"
        else if [parent_service_active] = null then "Missing Service"
        else if [parent_services_total] = 0 then "Zero Service"
        else if [parent_service_active] = "Yes" then "Has Service"
        else if [parent_service_active] = "No" then "Old Service Code"
        else "Unknown", Text.Type),
    
    AddedHasN0000UA = Table.AddColumn(AddedServiceCategory, "has_N0000UA", each
        if [has_N0000UA_count] > 0 then "Yes" else "No", Text.Type),
    
    // Clean up temporary columns
    FinalTable = Table.RemoveColumns(AddedHasN0000UA, 
        {"child_services_total", "parent_services_total", "has_N0000UA_count", "parent_service_active"})
in
    FinalTable

// ParentRates
// ParentRates  
let
    Source = Rate_Type_Facts,
    // Join current DWM rate types
    JoinedRateTypes = Table.NestedJoin(
        Source, {"Ratetype_Key"}, 
        CurrentRateTypes, {"Ratetype_Key"}, 
        "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(
        JoinedRateTypes, "RateTypeDetails", 
        {"IS_Current_Period", "Category_Code", "Ratetype"}),
    
    // Filter to current DWM rates, exclude N0000UA
    FilteredRates = Table.SelectRows(ExpandedRateTypes, each 
        [IS_Current_Period] = "Y" and 
        [Category_Code] = "DWM" and 
        [Ratetype] <> "N0000UA" and
        ([Suspended_Date] = null or [Suspended_Date] > DateTime.LocalNow())),
    
    // Join assessment info
    JoinedAssessment = Table.NestedJoin(
        FilteredRates, {"Assessment_Key"}, 
        Rate_Assessment, {"Assessment_Key"}, 
        "AssessmentInfo", JoinKind.Inner),
    ExpandedAssessment = Table.ExpandTableColumn(
        JoinedAssessment, "AssessmentInfo", 
        {"Assessment", "Vgnumber", "Ratepayer_Address_1"}, 
        {"Assessment", "Vgnumber", "RatePayer"}),
    
    // Join to filter only parent properties
    JoinedParents = Table.NestedJoin(
        ExpandedAssessment, {"Property_key"}, 
        MUDParentProperties, {"Property_key"}, 
        "ParentInfo", JoinKind.Inner),
    
    SelectedColumns = Table.SelectColumns(JoinedParents, 
        {"Property_key", "Ratetype", "Assessment", "Vgnumber", "RatePayer"})
in
    SelectedColumns

// ChildRates
// ChildRates
let
    Source = Rate_Type_Facts,
    // Join current DWM rate types  
    JoinedRateTypes = Table.NestedJoin(
        Source, {"Ratetype_Key"}, 
        CurrentRateTypes, {"Ratetype_Key"}, 
        "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(
        JoinedRateTypes, "RateTypeDetails", 
        {"IS_Current_Period", "Category_Code", "Ratetype"}),
    
    // Filter to current DWM rates, exclude N0000UA  
    FilteredRates = Table.SelectRows(ExpandedRateTypes, each 
        [IS_Current_Period] = "Y" and 
        [Category_Code] = "DWM" and 
        [Ratetype] <> "N0000UA" and
        ([Suspended_Date] = null or [Suspended_Date] > DateTime.LocalNow())),
    
    // Join assessment info
    JoinedAssessment = Table.NestedJoin(
        FilteredRates, {"Assessment_Key"}, 
        Rate_Assessment, {"Assessment_Key"}, 
        "AssessmentInfo", JoinKind.Inner),
    ExpandedAssessment = Table.ExpandTableColumn(
        JoinedAssessment, "AssessmentInfo", 
        {"Assessment", "Vgnumber", "Ratepayer_Address_1"}, 
        {"Assessment", "Vgnumber", "RatePayer"}),
    
    // Join to filter only child properties and add parent info
    JoinedChildren = Table.NestedJoin(
        ExpandedAssessment, {"Property_key"}, 
        ChildProperties, {"Property_key"}, 
        "ChildInfo", JoinKind.Inner),
    ExpandedChildren = Table.ExpandTableColumn(
        JoinedChildren, "ChildInfo", 
        {"Parent_Property_Key", "TPKLPAPROP", "Formatted_Address", "Suburb"}, 
        {"Parent_Property_Key", "Child_TPKLPAPROP", "Child_Formatted_Address", "Child_Suburb"}),
    
    SelectedColumns = Table.SelectColumns(ExpandedChildren, 
        {"Property_key", "Parent_Property_Key", "Child_TPKLPAPROP", "Child_Formatted_Address", "Child_Suburb", 
         "Ratetype", "Assessment", "Vgnumber", "RatePayer"})
in
    SelectedColumns

// SUDAudit
// SUDAudit
let
    Source = SUDProperties,
    
    // Left join services (to catch properties with no services)
    JoinedServices = Table.NestedJoin(
        Source, {"Property_key"}, 
        SUDServices, {"Property_key"}, 
        "Services", JoinKind.LeftOuter),
    ExpandedServices = Table.ExpandTableColumn(
        JoinedServices, "Services", 
        {"number_of_services", "number_of_red_services", "service_category"}),
    
    // Inner join rates (we only want properties that have rates)
    JoinedRates = Table.NestedJoin(
        ExpandedServices, {"Property_key"}, 
        SUDRates, {"Property_key"}, 
        "Rates", JoinKind.Inner),
    ExpandedRates = Table.ExpandTableColumn(
        JoinedRates, "Rates", 
        {"Ratetype", "Vgnumber", "RatePayer"}),
    
    // Left join bin services for audit date
    JoinedBinServices = Table.NestedJoin(
        ExpandedRates, {"Property_key"}, 
        Reg_BSERV_Bin_Services, {"Property_Key_LINK_TO_PROPERTY"}, 
        "BinServices", JoinKind.LeftOuter),
    ExpandedBinServices = Table.ExpandTableColumn(
        JoinedBinServices, "BinServices", 
        {"C_AUDIT_DATE"}, {"Audit_Date"}),
    
    // Replace null service values with defaults
    ReplacedNulls = Table.ReplaceValue(
        ExpandedBinServices, null, 0, Replacer.ReplaceValue, 
        {"number_of_services", "number_of_red_services"}),
    ReplacedServiceCategory = Table.ReplaceValue(
        ReplacedNulls, null, "Missing Service", Replacer.ReplaceValue, 
        {"service_category"}),
    
    // Add columns to match final output format
    AddedParentTPKL = Table.AddColumn(ReplacedServiceCategory, "parent_TPKLPAPROP", each [TPKLPAPROP], Int64.Type),
    AddedHasN0000UA = Table.AddColumn(AddedParentTPKL, "has_N0000UA", each "No", Text.Type),
    
    // Filter for audit conditions: properties with rates but no services OR old service codes
    FilteredRows = Table.SelectRows(AddedHasN0000UA, each 
        [number_of_services] = 0 or [service_category] = "Old Service Code"),
    
    // Select final columns to match report format
    FinalColumns = Table.SelectColumns(FilteredRows, {
        "parent_TPKLPAPROP", "TPKLPAPROP", "Formatted_Address", "Suburb", 
        "Assessment", "Vgnumber", "Ratetype", "RatePayer", "Audit_Date", 
        "service_category", "has_N0000UA"})
in
    FinalColumns

// MUDAudit
// MUDAudit
let
    Source = ChildRates,
    
    // Join parent property info via Parent_Property_Key
    JoinedParentInfo = Table.NestedJoin(
        Source, {"Parent_Property_Key"}, 
        MUDParentProperties, {"Property_key"}, 
        "ParentInfo", JoinKind.Inner),
    ExpandedParentInfo = Table.ExpandTableColumn(
        JoinedParentInfo, "ParentInfo", 
        {"TPKLPAPROP", "Formatted_Address", "Suburb"}, 
        {"Parent_TPKLPAPROP", "Parent_Formatted_Address", "Parent_Suburb"}),
    
    // Join parent service aggregation
    JoinedParentServices = Table.NestedJoin(
        ExpandedParentInfo, {"Parent_Property_Key"}, 
        ParentChildServiceAggregation, {"Property_key"}, 
        "Services", JoinKind.LeftOuter),
    ExpandedParentServices = Table.ExpandTableColumn(
        JoinedParentServices, "Services", 
        {"total_number_of_services", "service_category", "has_N0000UA"}),
    
    // Join bin services for audit date (using child property)
    JoinedBinServices = Table.NestedJoin(
        ExpandedParentServices, {"Property_key"}, 
        Reg_BSERV_Bin_Services, {"Property_Key_LINK_TO_PROPERTY"}, 
        "BinServices", JoinKind.LeftOuter),
    ExpandedBinServices = Table.ExpandTableColumn(
        JoinedBinServices, "BinServices", 
        {"C_AUDIT_DATE"}, {"Audit_Date"}),
    
    // Replace nulls with defaults
    ReplacedNulls = Table.ReplaceValue(
        ExpandedBinServices, null, 0, Replacer.ReplaceValue, 
        {"total_number_of_services"}),
    ReplacedServiceCategory = Table.ReplaceValue(
        ReplacedNulls, null, "Missing Service", Replacer.ReplaceValue, 
        {"service_category"}),
    ReplacedHasN0000UA = Table.ReplaceValue(
        ReplacedServiceCategory, null, "No", Replacer.ReplaceValue, 
        {"has_N0000UA"}),
    
    // Filter for MUD violations: child has rates but parent has no services OR old service codes
    FilteredRows = Table.SelectRows(ReplacedHasN0000UA, each 
        [total_number_of_services] = 0 or [service_category] = "Old Service Code"),
    
    // Select and rename columns to match SUDAudit structure
    FinalColumns = Table.SelectColumns(FilteredRows, {
        "Parent_TPKLPAPROP", "Child_TPKLPAPROP", "Child_Formatted_Address", "Child_Suburb", 
        "Assessment", "Vgnumber", "Ratetype", "RatePayer", "Audit_Date", 
        "service_category", "has_N0000UA"}),
    RenamedColumns = Table.RenameColumns(FinalColumns, {
        {"Parent_TPKLPAPROP", "parent_TPKLPAPROP"}, 
        {"Child_TPKLPAPROP", "TPKLPAPROP"}, 
        {"Child_Formatted_Address", "Formatted_Address"}, 
        {"Child_Suburb", "Suburb"}})
in
    RenamedColumns

// MUDSpecialN0000UA
// MUDSpecialN0000UA
let
    Source = ChildProperties,
    
    // Filter to active child properties
    FilteredActive = Table.SelectRows(Source, each [Status] = "C"),
    
    // Join parent property info to get parent TPKLPAPROP
    JoinedParentInfo = Table.NestedJoin(
        FilteredActive, {"Parent_Property_Key"}, 
        MUDParentProperties, {"Property_key"}, 
        "ParentInfo", JoinKind.Inner),
    ExpandedParentInfo = Table.ExpandTableColumn(
        JoinedParentInfo, "ParentInfo", 
        {"TPKLPAPROP"}, {"Parent_TPKLPAPROP"}),
    
    // Join child services - filter to N0000UA only
    JoinedChildServices = Table.NestedJoin(
        ExpandedParentInfo, {"Property_key"}, 
        Property_Services_Facts, {"Property_key"}, 
        "ChildServices", JoinKind.Inner),
    ExpandedChildServices = Table.ExpandTableColumn(
        JoinedChildServices, "ChildServices", 
        {"Service_Code", "Status"}, {"Service_Code", "Service_Status"}),
    FilteredN0000UA = Table.SelectRows(ExpandedChildServices, each 
        [Service_Code] = "N0000UA" and [Service_Status] = "C"),
    
    // Join child rates (must have rates)
    JoinedChildRates = Table.NestedJoin(
        FilteredN0000UA, {"Property_key"}, 
        ChildRates, {"Property_key"}, 
        "ChildRatesInfo", JoinKind.Inner),
    ExpandedChildRates = Table.ExpandTableColumn(
        JoinedChildRates, "ChildRatesInfo", 
        {"Vgnumber", "RatePayer", "Ratetype"}),
    
    // Join child bin services - filter to manual calculation
    JoinedChildBinServices = Table.NestedJoin(
        ExpandedChildRates, {"Property_key"}, 
        Reg_BSERV_Bin_Services, {"Property_Key_LINK_TO_PROPERTY"}, 
        "ChildBinServices", JoinKind.Inner),
    ExpandedChildBinServices = Table.ExpandTableColumn(
        JoinedChildBinServices, "ChildBinServices", 
        {"C_CALCULATION_METHOD", "C_AUDIT_DATE"}, 
        {"Child_Calc_Method", "Audit_Date"}),
    FilteredManualCalc = Table.SelectRows(ExpandedChildBinServices, each 
        Text.Start([Child_Calc_Method], 3) = "MAN"),
    
    // Join parent services (should be null or no current services)
    JoinedParentServices = Table.NestedJoin(
        FilteredManualCalc, {"Parent_Property_Key"}, 
        Property_Services_Facts, {"Property_key"}, 
        "ParentServices", JoinKind.LeftOuter),
    ExpandedParentServices = Table.ExpandTableColumn(
        JoinedParentServices, "ParentServices", 
        {"Service_Code", "Status"}, {"Parent_Service_Code", "Parent_Service_Status"}),
    FilteredNoParentServices = Table.SelectRows(ExpandedParentServices, each 
        [Parent_Service_Code] = null or [Parent_Service_Status] <> "C"),
    
    // Join parent bin services (should be null)
    JoinedParentBinServices = Table.NestedJoin(
        FilteredNoParentServices, {"Parent_Property_Key"}, 
        Reg_BSERV_Bin_Services, {"Property_Key_LINK_TO_PROPERTY"}, 
        "ParentBinServices", JoinKind.LeftOuter),
    ExpandedParentBinServices = Table.ExpandTableColumn(
        JoinedParentBinServices, "ParentBinServices", 
        {"C_CALCULATION_METHOD"}, {"Parent_Calc_Method"}),
    FilteredNoParentBinServices = Table.SelectRows(ExpandedParentBinServices, each 
        [Parent_Calc_Method] = null),
    
    // Join parent rates (should be null)
    JoinedParentRates = Table.NestedJoin(
        FilteredNoParentBinServices, {"Parent_Property_Key"}, 
        ParentRates, {"Property_key"}, 
        "ParentRates", JoinKind.LeftOuter),
    ExpandedParentRates = Table.ExpandTableColumn(
        JoinedParentRates, "ParentRates", 
        {"Ratetype"}, {"Parent_Ratetype"}),
    FilteredNoParentRates = Table.SelectRows(ExpandedParentRates, each 
        [Parent_Ratetype] = null),
    
    // Add constant columns and rename for final output
    AddedServiceCategory = Table.AddColumn(FilteredNoParentRates, "service_category", each "Child Has Service", Text.Type),
    AddedHasN0000UA = Table.AddColumn(AddedServiceCategory, "has_N0000UA", each "Yes", Text.Type),
    RenamedParentColumn = Table.RenameColumns(AddedHasN0000UA, {{"Parent_TPKLPAPROP", "parent_TPKLPAPROP"}}),
    
    // Select final columns to match report structure
    FinalColumns = Table.SelectColumns(RenamedParentColumn, {
        "parent_TPKLPAPROP", "TPKLPAPROP", "Formatted_Address", "Suburb", 
        "Assessment", "Vgnumber", "Ratetype", "RatePayer", "Audit_Date", 
        "service_category", "has_N0000UA"})
in
    FinalColumns

// BinRatesWithoutServices
// BinRatesWithoutServices
let
    Source = Table.Combine({SUDAudit, MUDAudit, MUDSpecialN0000UA}),
    #"Changed Type" = Table.TransformColumnTypes(Source,{{"Assessment", type text}, {"parent_TPKLPAPROP", Int64.Type}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{{"parent_TPKLPAPROP", "Parent TPK"}, {"TPKLPAPROP", "TPK"}, {"Formatted_Address", "Address"}, {"Ratetype", "Rate type"}, {"RatePayer", "Rate payer"}, {"Audit_Date", "Audit date"}, {"service_category", "Service category"}, {"has_N0000UA", "Has N0000UA?"}})
in
    #"Renamed Columns"
