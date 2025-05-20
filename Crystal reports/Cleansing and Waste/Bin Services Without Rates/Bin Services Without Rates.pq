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

// ParentProperties
// ParentProperties
let
    Source = #"Property",
    FilteredRows = Table.SelectRows(Source, each [Parent_Property_Key] = null)
in
    FilteredRows

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
        {"number_of_services", "number_of_red_services", "service_category"}),
    
    // Join bin services configuration
    JoinedBinServices = Table.NestedJoin(
        ExpandedServices, {"Property_key"}, 
        #"BinServicesInfo", {"Property_key"}, 
        "BinServices", JoinKind.LeftOuter),
    ExpandedBinServices = Table.ExpandTableColumn(
        JoinedBinServices, "BinServices", 
        {"calculation_method"}),
    
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
        {"total_child_rates", "total_child_rate_units"}),
    
    // Replace nulls with zeros for numeric columns
    ReplacedNulls = Table.ReplaceValue(
        ExpandedChildProps, null, 0, Replacer.ReplaceValue, 
        {"number_of_services", "number_of_red_services", "number_of_rates", 
         "rate_units", "total_child_rates", "total_child_rate_units"}),
    
    // Final filter for properties with services but no rates
    FilteredRows = Table.SelectRows(ReplacedNulls, each 
        [number_of_services] > 0 and
        [total_child_rate_units] = 0 and
        [number_of_rates] = 0)
in
    FilteredRows

// Rate_Assessment
// Rate_Assessment
let
    Source = Sql.Database(Server, Database),
    infodbo_Rate_Assessment = Source{[Schema="infodbo",Item="Rate_Assessment"]}[Data]
in
    infodbo_Rate_Assessment

// AssessmentInfo
// AssessmentInfo
let
    Source = #"Rate_Assessment",
    SelectColumns = Table.SelectColumns(Source, {"TFKLPAPROP", "Assessment", "Vgnumber"})
in
    SelectColumns

// Property_Services_Facts
// Property_Services_Facts
let
    Source = Sql.Database(Server, Database),
    infodbo_Property_Services_Facts = Source{[Schema="infodbo",Item="Property_Services_Facts"]}[Data]
in
    infodbo_Property_Services_Facts

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
        {"service_codes_list", each List.Min([Service_Code])},
        {"service_active_list", each List.Min([Is_Active])}
    }),
    AddedServiceCategory = Table.AddColumn(GroupedByProperty, "service_category", each if [service_active_list] = null then "Missing service" else if [number_of_services] = 0 then "Zero service" else if [service_active_list] = "Yes" then "Has service" else if [service_active_list] = "No" then "Old Service Code" else "Unknown", Text.Type),
    
    // Clean up temporary columns
    FinalTable = Table.RemoveColumns(AddedServiceCategory, 
        {"service_codes_list", "service_active_list"})
in
    FinalTable

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

// Rate_Type_Facts
// Rate_Type_Facts
let
    Source = Sql.Database(Server, Database),
    infodbo_Rate_Type_Facts = Source{[Schema="infodbo",Item="Rate_Type_Facts"]}[Data]
in
    infodbo_Rate_Type_Facts

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
            {"number_of_rates", each Table.RowCount(_)},
            {"rate_units", each List.Sum([Units])}
        })
in
    GroupedByProperty

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

// ChildProperties
// ChildProperties
let
    Source = #"Property", 
    FilteredRows = Table.SelectRows(Source, each [Parent_Property_Key] <> null)
in
    FilteredRows

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
    GroupedByParent = Table.Group(
        ReplacedNulls, {"Parent_Property_Key"}, {
            {"total_child_rate_units", each List.Sum([rate_units])},
            {"total_child_rates", each List.Sum([number_of_rates])}
        }),
    
    // Rename to match join field
    RenamedColumn = Table.RenameColumns(
        GroupedByParent, {{"Parent_Property_Key", "Property_key"}})
in
    RenamedColumn
