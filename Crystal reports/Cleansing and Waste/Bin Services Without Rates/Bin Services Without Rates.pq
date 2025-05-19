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
    FilteredRows = Table.SelectRows(Source, each ([Parent_Property_Key] = null) and ([Status] = "C"))
in
    FilteredRows

// PropertyServiceBase
// PropertyServiceBase
let
    Source = #"ParentProperties",
    JoinedChildren = Table.NestedJoin(Source, {"Property_key"}, #"ChildProperties", {"Parent_Property_Key"}, "ChildProps", JoinKind.LeftOuter),
    ExpandedChildProps = Table.ExpandTableColumn(JoinedChildren, "ChildProps", {"Property_key", "TPKLPAPROP", "Formatted_Address", "Suburb"}, {"child_Property_key", "child_TPKLPAPROP", "child_Formatted_Address", "child_Suburb"}),
    AddedCombinedProps = Table.AddColumn(ExpandedChildProps, "combined_Property_key", each 
        if [child_Property_key] = null then [Property_key] else [child_Property_key],Int64.Type),
    AddedCombinedTPKLPAPROP = Table.AddColumn(AddedCombinedProps, "combined_TPKLPAPROP", each 
        if [child_TPKLPAPROP] = null then [TPKLPAPROP] else [child_TPKLPAPROP],Int64.Type),
    AddedCombinedAddress = Table.AddColumn(AddedCombinedTPKLPAPROP, "combined_Formatted_Address", each 
        if [child_Formatted_Address] = null then [Formatted_Address] else [child_Formatted_Address],Text.Type),
    AddedCombinedSuburb = Table.AddColumn(AddedCombinedAddress, "combined_Suburb", each 
        if [child_Suburb] = null then [Suburb] else [child_Suburb],Text.Type),
    JoinedChildCounts = Table.NestedJoin(AddedCombinedSuburb, {"Property_key"}, #"PropertyChildCounts", {"Property_key"}, "ChildCounts", JoinKind.LeftOuter),
    ExpandedChildCounts = Table.ExpandTableColumn(JoinedChildCounts, "ChildCounts", {"number_of_all_child_properties", "number_of_child_properties"}),
    JoinedAssessment = Table.NestedJoin(ExpandedChildCounts, {"TPKLPAPROP"}, #"AssessmentInfo", {"TFKLPAPROP"}, "ParentAssessment", JoinKind.LeftOuter),
    ExpandedParentAssessment = Table.ExpandTableColumn(JoinedAssessment, "ParentAssessment", {"Assessment", "Vgnumber"}, {"parent_Assessment", "parent_Vgnumber"}),
    JoinedServices = Table.NestedJoin(ExpandedParentAssessment, {"Property_key"}, #"ServiceCategoryCalc", {"Property_key"}, "ServiceDetails", JoinKind.LeftOuter),
    ExpandedServices = Table.ExpandTableColumn(JoinedServices, "ServiceDetails", {"number_of_services", "number_of_red_services", "service_category"}),
    JoinedBinServices = Table.NestedJoin(ExpandedServices, {"Property_key"}, #"BinServicesInfo", {"Property_key"}, "BinServiceDetails", JoinKind.LeftOuter),
    ExpandedBinServices = Table.ExpandTableColumn(JoinedBinServices, "BinServiceDetails", {"calculation_method"}),
    JoinedParentRates = Table.NestedJoin(ExpandedBinServices, {"Property_key"}, #"ParentPropertyRates", {"Property_key"}, "ParentRates", JoinKind.LeftOuter),
    ExpandedParentRates = Table.ExpandTableColumn(JoinedParentRates, "ParentRates", {"number_of_rates"}, {"parent_number_of_rates"}),
    ReplacedNulls = Table.ReplaceValue(ExpandedParentRates, null, 0, Replacer.ReplaceValue, {"number_of_services", "number_of_red_services", "parent_number_of_rates"})
in
    ReplacedNulls

// ChildProperties
// ChildProperties
let
    Source = #"Property",
    FilteredRows = Table.SelectRows(Source, each ([Status] = "C"))
in
    FilteredRows

// PropertyChildCounts
// PropertyChildCounts
let
    Source = #"ParentProperties",
    // Join for all child properties counts
    JoinAllChildCounts = Table.NestedJoin(
        Source, {"Property_key"}, 
        #"AllChildPropertiesCounts", {"Property_key"}, 
        "AllChildCounts", JoinKind.LeftOuter),
    ExpandedAllChildCounts = Table.ExpandTableColumn(
        JoinAllChildCounts, "AllChildCounts", {"number_of_all_child_properties"}),
    
    // Join for filtered child properties counts
    JoinFilteredChildCounts = Table.NestedJoin(
        ExpandedAllChildCounts, {"Property_key"}, 
        #"FilteredChildPropertiesCounts", {"Property_key"}, 
        "FilteredChildCounts", JoinKind.LeftOuter),
    ExpandedFilteredChildCounts = Table.ExpandTableColumn(
        JoinFilteredChildCounts, "FilteredChildCounts", {"number_of_child_properties"}),
    
    // Replace nulls with zeros
    ReplacedNulls = Table.ReplaceValue(
        ExpandedFilteredChildCounts, null, 0, Replacer.ReplaceValue, 
        {"number_of_all_child_properties", "number_of_child_properties"})
in
    ReplacedNulls

// AllChildPropertiesCounts
// AllChildPropertiesCounts
let
    Source = #"ChildProperties",
    GroupedByParent = Table.Group(Source, {"Parent_Property_Key"}, {
        {"number_of_all_child_properties", each Table.RowCount(_)}
    }),
    RenamedColumn = Table.RenameColumns(GroupedByParent, {{"Parent_Property_Key", "Property_key"}})
in
    RenamedColumn

// FilteredChildProperties
// FilteredChildProperties
let
    Source = #"ChildProperties",
    FilteredRows = Table.SelectRows(Source, each 
        not Text.Contains([Formatted_Address], "Car Space") and 
        not Text.Contains([Formatted_Address], "Storage"))
in
    FilteredRows

// FilteredChildPropertiesCounts
// FilteredChildPropertiesCounts
let
    Source = #"FilteredChildProperties",
    GroupedByParent = Table.Group(Source, {"Parent_Property_Key"}, {
        {"number_of_child_properties", each Table.RowCount(_)}
    }),
    RenamedColumn = Table.RenameColumns(GroupedByParent, {{"Parent_Property_Key", "Property_key"}})
in
    RenamedColumn

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

// FilteredActiveServices
// FilteredActiveServices
let
    Source = #"Property_Services_Facts",
    FilteredRows = Table.SelectRows(Source, each 
        ([Status] = "C" or [Removal_Date] > DateTime.LocalNow()) and 
        (Text.Start([Service_Code], 1) = "R" or 
         Text.Start([Service_Code], 1) = "G" or 
         Text.Start([Service_Code], 1) = "N"))
in
    FilteredRows

// ServiceWithCodeInfo
// ServiceWithCodeInfo
let
    Source = #"FilteredActiveServices",
    JoinedServiceCodes = Table.NestedJoin(Source, {"Service_Code"}, 
        #"Property_Service_Codes_Custom", {"Service_Code"}, 
        "ServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedServiceCodes = Table.ExpandTableColumn(JoinedServiceCodes, 
        "ServiceCodeInfo", {"Is_Active"}, {"service_Is_Active"}),
    FilteredBinServices = Table.SelectRows(ExpandedServiceCodes, each 
        Text.Start([Service_Code], 1) = "R" or 
        (Text.Start([Service_Code], 1) = "G" and Text.Start([service_Is_Active], 2) = "No") or 
        Text.Start([Service_Code], 1) = "N")
in
    FilteredBinServices

// Property_Service_Codes_Custom
// Property_Service_Codes_Custom
let
    Source = Sql.Database(Server, Database),
    infodbo_Property_Service_Codes_Custom = Source{[Schema="infodbo",Item="Property_Service_Codes_Custom"]}[Data]
in
    infodbo_Property_Service_Codes_Custom

// AllServiceCount
// AllServiceCount
let
    Source = #"ServiceWithCodeInfo",
    GroupedByProperty = Table.Group(Source, {"Property_key"}, {
        {"number_of_services", each List.Sum([Number_of_Services])}
    })
in
    GroupedByProperty

// PropertyServiceDetails
// PropertyServiceDetails
let
    Source = #"AllServiceCount",
    // Join red services count
    JoinedRedServices = Table.NestedJoin(
        Source, {"Property_key"}, 
        #"RedServices", {"Property_key"}, 
        "RedServices", JoinKind.LeftOuter),
    ExpandedRedServices = Table.ExpandTableColumn(
        JoinedRedServices, "RedServices", {"number_of_red_services"}),
    
    // Join service codes for category determination later
    JoinedServiceCodes = Table.NestedJoin(
        ExpandedRedServices, {"Property_key"}, 
        #"ServiceCodesByProperty", {"Property_key"}, 
        "service_codes", JoinKind.LeftOuter),
    
    // Replace nulls with zeros
    ReplacedNulls = Table.ReplaceValue(
        JoinedServiceCodes, null, 0, Replacer.ReplaceValue, 
        {"number_of_services", "number_of_red_services"})
in
    ReplacedNulls

// RedServices
// RedServices
let
    Source = #"FilteredActiveServices",
    FilteredRows = Table.SelectRows(Source, each Text.Start([Service_Code], 1) = "R"),
    GroupedByProperty = Table.Group(FilteredRows, {"Property_key"}, {
        {"number_of_red_services", each List.Sum([Number_of_Services])}
    })
in
    GroupedByProperty

// ServiceCodesByProperty
// ServiceCodesByProperty
let
    Source = #"ServiceWithCodeInfo",
    SelectColumns = Table.SelectColumns(Source, {"Property_key", "Service_Code", "service_Is_Active"})
in
    SelectColumns

// ServiceCategoryCalc
// ServiceCategoryCalc
let
    Source = #"PropertyServiceDetails",
    ExpandedServiceCodes = Table.ExpandTableColumn(Source, "service_codes", {"Service_Code", "service_Is_Active"}, {"Service_Code", "service_Is_Active"}),
    AddedServiceCategory = Table.AddColumn(ExpandedServiceCodes, "service_category", each 
        if [service_Is_Active] = null then "Missing Service"
        else if [number_of_services] = 0 then "Zero Service"
        else if [service_Is_Active] = "Yes" then "Has Service"
        else if Text.Start([service_Is_Active], 2) = "No" then "Old Service Code"
        else "Unknown"),
    GroupedBack = Table.Group(AddedServiceCategory, {"Property_key", "number_of_services", "number_of_red_services"}, {
        {"service_category", each List.Min([service_category])}
    })
in
    GroupedBack

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
        if [C_CALCULATION_METHOD] = null then "None" else Text.Start([C_CALCULATION_METHOD], 4)),
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

// ParentPropertyRates
// ParentPropertyRates
let
    Source = #"Rate_Type_Facts",
    JoinedRateTypes = Table.NestedJoin(Source, {"Ratetype_Key"}, CurrentRateTypes, {"Ratetype_Key"}, "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(JoinedRateTypes, "RateTypeDetails", {"IS_Current_Period", "Category_Code"}, {"IS_Current_Period", "Category_Code"}),
    FilteredRows = Table.SelectRows(ExpandedRateTypes, each 
        ([IS_Current_Period] = "Y") and 
        ([Category_Code] = "DWM") and 
        ([Suspended_Date] = null or [Suspended_Date] > DateTime.LocalNow())),
    RenamedColumns = Table.RenameColumns(FilteredRows, {{"Units", "rate_units"}}),
    GroupedByProperty = Table.Group(RenamedColumns, {"Property_key"}, {
        {"number_of_rates", each Table.RowCount(_)},
        {"rate_units", each List.Sum([rate_units])}
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

// BinServicesWithoutRates
// BinServicesWithoutRates
let
    Source = #"PropertyServiceBase",
    JoinedChildRateTotals = Table.NestedJoin(Source, {"Property_key"}, #"ChildRatesTotals", {"Property_key"}, "ChildRateTotals", JoinKind.LeftOuter),
    ExpandedChildRateTotals = Table.ExpandTableColumn(JoinedChildRateTotals, "ChildRateTotals", {"total_child_rate_units", "total_child_rates"}),
    ReplacedNulls = Table.ReplaceValue(ExpandedChildRateTotals, null, 0, Replacer.ReplaceValue, {"total_child_rate_units", "total_child_rates"}),
    FilteredRows = Table.SelectRows(ReplacedNulls, each 
        [number_of_services] > 0 and
        [total_child_rate_units] = 0 and
        [parent_number_of_rates] = 0 and
        not Text.Contains([Formatted_Address], "Car Space") and
        not Text.Contains([Formatted_Address], "Storage"))
in
    FilteredRows

// ChildRatesTotals
// ChildRatesTotals
let
    Source = #"PropertyServiceBase",
    FilteredRows = Table.SelectRows(Source, each [child_Property_key] <> null),
    JoinedChildRates = Table.NestedJoin(FilteredRows, {"child_Property_key"}, #"ParentPropertyRates", {"Property_key"}, "ChildRates", JoinKind.LeftOuter),
    ExpandedChildRates = Table.ExpandTableColumn(JoinedChildRates, "ChildRates", {"rate_units", "number_of_rates"}, {"child_rate_units", "child_number_of_rates"}),
    ReplacedNulls = Table.ReplaceValue(ExpandedChildRates, null, 0, Replacer.ReplaceValue, {"child_rate_units", "child_number_of_rates"}),
    GroupedByParent = Table.Group(ReplacedNulls, {"Property_key"}, {
        {"total_child_rate_units", each List.Sum([child_rate_units])},
        {"total_child_rates", each List.Sum([child_number_of_rates])}
    })
in
    GroupedByParent
