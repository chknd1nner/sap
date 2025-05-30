// Server
"spacjjxnnbdenmioeko67ffj4e-x2fqwohpz4ae5f4vgvdmzxxmwi.datawarehouse.fabric.microsoft.com" meta [IsParameterQuery=true, Type="Any", IsParameterQueryRequired=true]

// Database
"DIMSU_Lakehouse_Bronze" meta [IsParameterQuery=true, Type="Any", IsParameterQueryRequired=true]

// Property
// Property
let
    Source = Sql.Database(Server, Database),
    pathway_property = Source{[Schema="pathway",Item="property"]}[Data],
    #"Filtered Rows" = Table.SelectRows(pathway_property, each [property_type] <> "STRCARSPC " and [property_type] <> "UTILITY   " and [status] = "C"),
    #"Removed Other Columns" = Table.SelectColumns(#"Filtered Rows",{"property_key", "formatted_address", "address", "suburb", "street", "status", "tpklpaprop", "parent_property_key", "parent_property_address", "formatted_owner_name", "owners_na_slab_1", "owners_na_slab_2", "owners_na_slab_3", "owners_na_slab_4", "owners_na_slab_5", "assessment", "assessment_key"})
in
    #"Removed Other Columns"

// ParentProperties
// ParentProperties
let
    Source = #"Property",
    FilteredRows = Table.SelectRows(Source, each [parent_property_key] = null)
in
    FilteredRows

// BinServicesWithoutRates
// BinServicesWithoutRates
let
    // Start with parent properties
    Source = #"ParentProperties",
    
    // Join assessment info for properties
    JoinedAssessment = Table.NestedJoin(
        Source, {"tpklpaprop"}, 
        #"AssessmentInfo", {"tfklpaprop"}, 
        "AssessmentInfo", JoinKind.LeftOuter),
    ExpandedAssessment = Table.ExpandTableColumn(
        JoinedAssessment, "AssessmentInfo", 
        {"vgnumber"},{"vgnumber"}),
    
    // Join property services 
    JoinedServices = Table.NestedJoin(
        ExpandedAssessment, {"property_key"}, 
        #"PropertyServices", {"property_key"}, 
        "Services", JoinKind.LeftOuter),
    ExpandedServices = Table.ExpandTableColumn(
        JoinedServices, "Services", 
        {"number_of_services", "number_of_red_services", "service_category"}, {"number_of_services", "number_of_red_services", "service_category"}),
    
    // Join bin services configuration
    JoinedBinServices = Table.NestedJoin(
        ExpandedServices, {"property_key"}, 
        #"BinServicesInfo", {"property_key"}, 
        "BinServices", JoinKind.LeftOuter),
    ExpandedBinServices = Table.ExpandTableColumn(
        JoinedBinServices, "BinServices", 
        {"calculation_method"}, {"calculation_method"}),
    
    // Join property rates information
    JoinedRates = Table.NestedJoin(
        ExpandedBinServices, {"property_key"}, 
        #"PropertyRates", {"property_key"}, 
        "Rates", JoinKind.LeftOuter),
    ExpandedRates = Table.ExpandTableColumn(
        JoinedRates, "Rates", 
        {"number_of_rates", "rate_units"}),
    
    // Join child properties data
    JoinedChildProps = Table.NestedJoin(
        ExpandedRates, {"property_key"}, 
        #"ChildRatesTotals", {"property_key"}, 
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
    #"Trimmed Text" = Table.TransformColumns(FilteredRows,{{"formatted_owner_name", Text.TrimEnd, type text}}),
    #"Changed Type" = Table.TransformColumnTypes(#"Trimmed Text",{{"assessment", type text}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{{"formatted_address", "Formatted address"}, {"tpklpaprop", "TPK"}, {"formatted_owner_name", "Owner(s)"}, {"service_category", "Service category"}, {"calculation_method", "Calculation method"}})
in
    #"Renamed Columns"

// Rate_Assessment
// Rate_Assessment
let
    Source = Sql.Database(Server, Database),
    pathway_rate_assessment = Source{[Schema="pathway",Item="rate_assessment"]}[Data]
in
    pathway_rate_assessment

// AssessmentInfo
// AssessmentInfo
let
    Source = #"Rate_Assessment",
    SelectColumns = Table.SelectColumns(Source, {"tfklpaprop", "assessment", "vgnumber"})
in
    SelectColumns

// Property_Services_Facts
// Property_Services_Facts
let
    Source = Sql.Database(Server, Database),
    pathway_property_services_facts = Source{[Schema="pathway",Item="property_services_facts"]}[Data],
    #"Filtered Rows" = Table.SelectRows(pathway_property_services_facts, each ([status] = "C"))
in
    #"Filtered Rows"

// PropertyServices
// PropertyServices
let
    Source = #"Property_Services_Facts",
    // Filter to active services of interest
    FilteredActiveServices = Table.SelectRows(Source, each 
        ([status] = "C" or [removal_date] > DateTime.LocalNow()) and 
        (Text.Start([service_code], 1) = "R" or 
         Text.Start([service_code], 1) = "G" or 
         Text.Start([service_code], 1) = "N")),
    
    // Join service code info
    JoinedServiceCodes = Table.NestedJoin(FilteredActiveServices, {"service_code"}, 
        #"Property_Service_Codes_Custom", {"service_code"}, 
        "ServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedServiceCodes = Table.ExpandTableColumn(JoinedServiceCodes, 
        "ServiceCodeInfo", {"is_active"}, {"is_active"}),
    
    // Apply bin service specific filter  
    FilteredBinServices = Table.SelectRows(ExpandedServiceCodes, each 
        Text.Start([service_code], 1) = "R" or 
        (Text.Start([service_code], 1) = "G" and [is_active] = "No") or 
        Text.Start([service_code], 1) = "N"),
    
    // Add dummy column for red services calculation that will fold to SQL
    AddedRedServiceCount = Table.AddColumn(FilteredBinServices, "red_service_count", each
        if Text.Start([service_code], 1) = "R" then [number_of_services] else 0, Int64.Type),
        
    // Calculate service metrics by property
    GroupedByProperty = Table.Group(AddedRedServiceCount, {"property_key"}, {
        {"number_of_services", each List.Sum([number_of_services]), Int64.Type},
        {"number_of_red_services", each List.Sum([red_service_count]), Int64.Type},
        {"service_codes_list", each List.Min([service_code]), Text.Type},
        {"service_active_list", each List.Min([is_active]), Text.Type}
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
    pathway_property_service_codes_custom = Source{[Schema="pathway",Item="property_service_codes_custom"]}[Data]
in
    pathway_property_service_codes_custom

// Reg_BSERV_Bin_Services
// Reg_BSERV_Bin_Services
let
    Source = Sql.Database(Server, Database),
    pathway_reg_bserv_bin_services = Source{[Schema="pathway",Item="reg_bserv_bin_services"]}[Data]
in
    pathway_reg_bserv_bin_services

// BinServicesInfo
// BinServicesInfo
let
    Source = #"Reg_BSERV_Bin_Services",
    SelectColumns = Table.SelectColumns(Source, {"property_key_link_to_property", "c_calculation_method"}),
    RenamedColumns = Table.RenameColumns(SelectColumns, {{"property_key_link_to_property", "property_key"}}),
    AddedCalculationMethod = Table.AddColumn(RenamedColumns, "calculation_method", each 
        if [c_calculation_method] = null then "None" else Text.Start([c_calculation_method], 4), Text.Type),
    RemovedOrigColumn = Table.RemoveColumns(AddedCalculationMethod, {"c_calculation_method"})
in
    RemovedOrigColumn

// Rate_Type_Facts
// Rate_Type_Facts
let
    Source = Sql.Database(Server, Database),
    pathway_rate_type_facts = Source{[Schema="pathway",Item="rate_type_facts"]}[Data]
in
    pathway_rate_type_facts

// PropertyRates
// PropertyRates
let
    Source = #"Rate_Type_Facts",
    // Join rate types to get only current DWM rates
    JoinedRateTypes = Table.NestedJoin(
        Source, {"ratetype_key"}, 
        #"CurrentRateTypes", {"ratetype_key"}, 
        "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(
        JoinedRateTypes, "RateTypeDetails", 
        {"is_current_period", "category_code"}),
    
    // Filter to active, unsuspended rates
    FilteredRows = Table.SelectRows(
        ExpandedRateTypes, each 
            [is_current_period] = "Y" and 
            [category_code] = "DWM" and 
            ([suspended_date] = null or [suspended_date] > DateTime.LocalNow())),
    
    // Group by property for rate summaries
    GroupedByProperty = Table.Group(
        FilteredRows, {"property_key"}, {
            {"number_of_rates", each Table.RowCount(_), Int64.Type},
            {"rate_units", each List.Sum([units]), Int64.Type}
        })
in
    GroupedByProperty

// Rate_Type
// Rate_Type
let
    Source = Sql.Database(Server, Database),
    pathway_rate_type = Source{[Schema="pathway",Item="rate_type"]}[Data]
in
    pathway_rate_type

// CurrentRateTypes
// CurrentRateTypes
let
    Source = #"Rate_Type",
    FilteredRows = Table.SelectRows(Source, each ([is_current_period] = "Y") and ([category_code] = "DWM")),
    AddedBinType = Table.AddColumn(FilteredRows, "rate_bin_type", each Text.Middle([ratetype], 3, 3) ),
    AddedFrequency = Table.AddColumn(AddedBinType, "rate_frequency", each Text.End([ratetype],2), Int16.Type)
in
    AddedFrequency

// ChildProperties
// ChildProperties
let
    Source = #"Property", 
    FilteredRows = Table.SelectRows(Source, each [parent_property_key] <> null)
in
    FilteredRows

// ChildRatesTotals
// ChildRatesTotals
let
    Source = #"ChildProperties",
    // Join rates for child properties
    JoinedRates = Table.NestedJoin(
        Source, {"property_key"}, 
        #"PropertyRates", {"property_key"}, 
        "Rates", JoinKind.LeftOuter),
    ExpandedRates = Table.ExpandTableColumn(
        JoinedRates, "Rates", 
        {"rate_units", "number_of_rates"}),
    
    // Replace nulls with zeros
    ReplacedNulls = Table.ReplaceValue(
        ExpandedRates, null, 0, Replacer.ReplaceValue, 
        {"rate_units", "number_of_rates"}),
    
    // Group by parent property for totals
    GroupedByParent = Table.Group(ReplacedNulls, {"parent_property_key"}, {{"total_child_rate_units", each List.Sum([rate_units]), type nullable number}, {"total_child_rates", each List.Sum([number_of_rates]), type nullable number}, {"total_child_props", each Table.RowCount(_), Int64.Type}}),
    
    // Rename to match join field
    RenamedColumn = Table.RenameColumns(
        GroupedByParent, {{"parent_property_key", "property_key"}})
in
    RenamedColumn

// SUDProperties
// SUDProperties  
let
    Source = ParentProperties,
    // Left join to find properties that have children
    JoinedChildren = Table.NestedJoin(
        Source, {"property_key"}, 
        Property, {"parent_property_key"}, 
        "Children", JoinKind.LeftOuter),
    ExpandedChildren = Table.ExpandTableColumn(
        JoinedChildren, "Children", 
        {"property_key"}, {"child_property_key"}),
    // Filter to properties with no children (SUDs)
    FilteredSUDs = Table.SelectRows(ExpandedChildren, each [child_property_key] = null),
    RemovedChildColumn = Table.RemoveColumns(FilteredSUDs, {"child_property_key"})
in
    RemovedChildColumn

// SUDAudit
// SUDAudit
let
    Source = SUDProperties,
    
    // Left join services (to catch properties with no services)
    JoinedServices = Table.NestedJoin(
        Source, {"property_key"}, 
        SUDServices, {"property_key"}, 
        "Services", JoinKind.LeftOuter),
    ExpandedServices = Table.ExpandTableColumn(
        JoinedServices, "Services", 
        {"number_of_services", "number_of_red_services", "service_category"}),
    
    // Inner join rates (we only want properties that have rates)
    JoinedRates = Table.NestedJoin(
        ExpandedServices, {"property_key"}, 
        SUDRates, {"property_key"}, 
        "Rates", JoinKind.Inner),
    ExpandedRates = Table.ExpandTableColumn(
        JoinedRates, "Rates", 
        {"ratetype", "vgnumber", "ratepayer"}, {"ratetype", "vgnumber", "ratepayer"}),
    
    // Left join bin services for audit date
    JoinedBinServices = Table.NestedJoin(
        ExpandedRates, {"property_key"}, 
        Reg_BSERV_Bin_Services, {"property_key_link_to_property"}, 
        "BinServices", JoinKind.LeftOuter),
    ExpandedBinServices = Table.ExpandTableColumn(
        JoinedBinServices, "BinServices", 
        {"c_audit_date"}, {"audit_date"}),
    
    // Replace null service values with defaults
    ReplacedNulls = Table.ReplaceValue(
        ExpandedBinServices, null, 0, Replacer.ReplaceValue, 
        {"number_of_services", "number_of_red_services"}),
    ReplacedServiceCategory = Table.ReplaceValue(
        ReplacedNulls, null, "Missing Service", Replacer.ReplaceValue, 
        {"service_category"}),
    
    // Add columns to match final output format
    AddedParentTPKL = Table.AddColumn(ReplacedServiceCategory, "parent_tpklpaprop", each [tpklpaprop], Int64.Type),
    AddedHasN0000UA = Table.AddColumn(AddedParentTPKL, "has_n0000ua", each "No", Text.Type),
    
    // Filter for audit conditions: properties with rates but no services OR old service codes
    FilteredRows = Table.SelectRows(AddedHasN0000UA, each 
        [number_of_services] = 0 or [service_category] = "Old Service Code"),
    
    // Select final columns to match report format
    FinalColumns = Table.SelectColumns(FilteredRows, {
        "parent_tpklpaprop", "tpklpaprop", "formatted_address", "suburb", 
        "assessment", "vgnumber", "ratetype", "ratepayer", "audit_date", 
        "service_category", "has_n0000ua"})
in
    FinalColumns

// SUDServices
// SUDServices
let
    Source = Property_Services_Facts,
    // Filter to active services of interest (same logic as original)
    FilteredActiveServices = Table.SelectRows(Source, each 
        ([status] = "C" or [removal_date] > DateTime.LocalNow()) and 
        (Text.Start([service_code], 1) = "R" or 
         Text.Start([service_code], 1) = "G" or 
         Text.Start([service_code], 1) = "N")),
    
    // Join service code info for G service filtering
    JoinedServiceCodes = Table.NestedJoin(FilteredActiveServices, {"service_code"}, 
        Property_Service_Codes_Custom, {"service_code"}, 
        "ServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedServiceCodes = Table.ExpandTableColumn(JoinedServiceCodes, 
        "ServiceCodeInfo", {"is_active"}, {"is_active"}),
    
    // Apply bin service specific filter (R services, inactive G services, N services)
    FilteredBinServices = Table.SelectRows(ExpandedServiceCodes, each 
        Text.Start([service_code], 1) = "R" or 
        (Text.Start([service_code], 1) = "G" and [is_active] = "No") or 
        Text.Start([service_code], 1) = "N"),
    
    // Exclude N0000UA from service counts
    FilteredExcludingUA = Table.SelectRows(FilteredBinServices, each [service_code] <> "N0000UA"),
    
    // Add dummy columns for aggregation that will fold to SQL
    AddedRedServiceCount = Table.AddColumn(FilteredExcludingUA, "red_service_count", each
        if Text.Start([service_code], 1) = "R" then [number_of_services] else 0, Int64.Type),
    
    // Group by property for service summaries
    GroupedByProperty = Table.Group(AddedRedServiceCount, {"property_key"}, {
        {"number_of_services", each List.Sum([number_of_services]), Int64.Type},
        {"number_of_red_services", each List.Sum([red_service_count]), Int64.Type},
        {"service_active_list", each List.Min([is_active]), Text.Type}
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
        Source, {"ratetype_key"}, 
        Rate_Type, {"ratetype_key"}, 
        "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(
        JoinedRateTypes, "RateTypeDetails", 
        {"is_current_period", "category_code", "ratetype"}, {"is_current_period", "category_code", "ratetype"}),
    
    // Filter to current DWM rates, exclude N0000UA
    FilteredRates = Table.SelectRows(ExpandedRateTypes, each 
        [is_current_period] = "Y" and 
        [category_code] = "DWM" and 
        [ratetype] <> "N0000UA" and
        ([suspended_date] = null or [suspended_date] > DateTime.LocalNow())),
    
    // Join assessment info
    JoinedAssessment = Table.NestedJoin(
        FilteredRates, {"assessment_key"}, 
        Rate_Assessment, {"assessment_key"}, 
        "AssessmentInfo", JoinKind.Inner),
    ExpandedAssessment = Table.ExpandTableColumn(
        JoinedAssessment, "AssessmentInfo", 
        {"assessment", "vgnumber", "ratepayer_address_1"}, 
        {"assessment", "vgnumber", "ratepayer"}),
    
    SelectedColumns = Table.SelectColumns(ExpandedAssessment, 
        {"property_key", "ratetype", "assessment", "vgnumber", "ratepayer"})
in
    SelectedColumns

// ChildRates
// ChildRates
let
    Source = Rate_Type_Facts,
    // Join current DWM rate types  
    JoinedRateTypes = Table.NestedJoin(
        Source, {"ratetype_key"}, 
        CurrentRateTypes, {"ratetype_key"}, 
        "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(
        JoinedRateTypes, "RateTypeDetails", 
        {"is_current_period", "category_code", "ratetype"}, {"is_current_period", "category_code", "ratetype"}),
    
    // Filter to current DWM rates, exclude N0000UA  
    FilteredRates = Table.SelectRows(ExpandedRateTypes, each 
        [is_current_period] = "Y" and 
        [category_code] = "DWM" and 
        [ratetype] <> "N0000UA" and
        ([suspended_date] = null or [suspended_date] > DateTime.LocalNow())),
    
    // Join assessment info
    JoinedAssessment = Table.NestedJoin(
        FilteredRates, {"assessment_key"}, 
        Rate_Assessment, {"assessment_key"}, 
        "AssessmentInfo", JoinKind.Inner),
    ExpandedAssessment = Table.ExpandTableColumn(
        JoinedAssessment, "AssessmentInfo", 
        {"assessment", "vgnumber", "ratepayer_address_1"}, 
        {"assessment", "vgnumber", "ratepayer"}),
    
    // Join to filter only child properties and add parent info
    JoinedChildren = Table.NestedJoin(
        ExpandedAssessment, {"property_key"}, 
        ChildProperties, {"property_key"}, 
        "ChildInfo", JoinKind.Inner),
    ExpandedChildren = Table.ExpandTableColumn(
        JoinedChildren, "ChildInfo", 
        {"parent_property_key", "tpklpaprop", "formatted_address", "suburb"}, 
        {"parent_property_key", "child_tpklpaprop", "child_formatted_address", "child_suburb"}),
    
    SelectedColumns = Table.SelectColumns(ExpandedChildren, 
        {"property_key", "parent_property_key", "child_tpklpaprop", "child_formatted_address", "child_suburb", 
         "ratetype", "assessment", "vgnumber", "ratepayer"})
in
    SelectedColumns

// MUDAudit
// MUDAudit
let
    Source = ChildRates,
    
    // Join parent property info via Parent_Property_Key
    JoinedParentInfo = Table.NestedJoin(
        Source, {"parent_property_key"}, 
        MUDParentProperties, {"property_key"}, 
        "ParentInfo", JoinKind.Inner),
    ExpandedParentInfo = Table.ExpandTableColumn(
        JoinedParentInfo, "ParentInfo", 
        {"tpklpaprop", "formatted_address", "suburb"}, 
        {"parent_tpklpaprop", "parent_formatted_address", "parent_suburb"}),
    
    // Join parent service aggregation
    JoinedParentServices = Table.NestedJoin(
        ExpandedParentInfo, {"parent_property_key"}, 
        ParentChildServiceAggregation, {"property_key"}, 
        "Services", JoinKind.LeftOuter),
    ExpandedParentServices = Table.ExpandTableColumn(
        JoinedParentServices, "Services", 
        {"total_number_of_services", "service_category", "has_n0000ua"}),
    
    // Join bin services for audit date (using child property)
    JoinedBinServices = Table.NestedJoin(
        ExpandedParentServices, {"property_key"}, 
        Reg_BSERV_Bin_Services, {"property_key_link_to_property"}, 
        "BinServices", JoinKind.LeftOuter),
    ExpandedBinServices = Table.ExpandTableColumn(
        JoinedBinServices, "BinServices", 
        {"c_audit_date"}, {"audit_date"}),
    
    // Replace nulls with defaults
    ReplacedNulls = Table.ReplaceValue(
        ExpandedBinServices, null, 0, Replacer.ReplaceValue, 
        {"total_number_of_services"}),
    ReplacedServiceCategory = Table.ReplaceValue(
        ReplacedNulls, null, "Missing Service", Replacer.ReplaceValue, 
        {"service_category"}),
    ReplacedHasN0000UA = Table.ReplaceValue(
        ReplacedServiceCategory, null, "No", Replacer.ReplaceValue, 
        {"has_n0000ua"}),
    
    // Filter for MUD violations: child has rates but parent has no services OR old service codes
    FilteredRows = Table.SelectRows(ReplacedHasN0000UA, each 
        [total_number_of_services] = 0 or [service_category] = "Old Service Code"),
    
    // Select and rename columns to match SUDAudit structure
    FinalColumns = Table.SelectColumns(FilteredRows, {
        "parent_tpklpaprop", "child_tpklpaprop", "child_formatted_address", "child_suburb", 
        "assessment", "vgnumber", "ratetype", "ratepayer", "audit_date", 
        "service_category", "has_n0000ua"}),
    RenamedColumns = Table.RenameColumns(FinalColumns, {
        {"parent_tpklpaprop", "parent_tpklpaprop"}, 
        {"child_tpklpaprop", "tpklpaprop"}, 
        {"child_formatted_address", "formatted_address"}, 
        {"child_suburb", "suburb"}})
in
    RenamedColumns

// MUDParentProperties
// MUDParentProperties
let
    Source = ParentProperties,
    // Inner join with child properties to find parents that actually have children
    JoinedChildren = Table.NestedJoin(
        Source, {"property_key"}, 
        ChildProperties, {"parent_property_key"}, 
        "Children", JoinKind.Inner),
    ExpandedChildren = Table.ExpandTableColumn(
        JoinedChildren, "Children", 
        {"property_key"}, {"child_property_key"}),
    // Group by parent to get distinct parent properties (since one parent can have multiple children)
    GroupedByParent = Table.Group(ExpandedChildren, 
        {"property_key", "tpklpaprop", "formatted_address", "suburb", "status"}, {})
in
    GroupedByParent

// ParentChildServiceAggregation
// ParentChildServiceAggregation
let
    Source = MUDParentProperties,
    
    // Join all children for each parent
    JoinedChildren = Table.NestedJoin(
        Source, {"property_key"}, 
        ChildProperties, {"parent_property_key"}, 
        "Children", JoinKind.LeftOuter),
    ExpandedChildren = Table.ExpandTableColumn(
        JoinedChildren, "Children", 
        {"property_key", "tpklpaprop", "formatted_address", "suburb"}, 
        {"child_property_key", "child_tpklpaprop", "child_formatted_address", "child_suburb"}),
    
    // Join parent services
    JoinedParentServices = Table.NestedJoin(
        ExpandedChildren, {"property_key"}, 
        Property_Services_Facts, {"property_key"}, 
        "ParentServices", JoinKind.LeftOuter),
    ExpandedParentServices = Table.ExpandTableColumn(
        JoinedParentServices, "ParentServices", 
        {"service_code", "number_of_services", "status", "removal_date"}, 
        {"parent_service_code", "parent_number_of_services", "parent_service_status", "parent_removal_date"}),
    
    // Join child services
    JoinedChildServices = Table.NestedJoin(
        ExpandedParentServices, {"child_property_key"}, 
        Property_Services_Facts, {"property_key"}, 
        "ChildServices", JoinKind.LeftOuter),
    ExpandedChildServices = Table.ExpandTableColumn(
        JoinedChildServices, "ChildServices", 
        {"service_code", "number_of_services", "status", "removal_date"}, 
        {"child_service_code", "child_number_of_services", "child_service_status", "child_removal_date"}),
    
    // Join parent service code info
    JoinedParentServiceCodes = Table.NestedJoin(
        ExpandedChildServices, {"parent_service_code"}, 
        Property_Service_Codes_Custom, {"service_code"}, 
        "ParentServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedParentServiceCodes = Table.ExpandTableColumn(
        JoinedParentServiceCodes, "ParentServiceCodeInfo", 
        {"is_active"}, {"parent_is_active"}),
    
    // Join child service code info  
    JoinedChildServiceCodes = Table.NestedJoin(
        ExpandedParentServiceCodes, {"child_service_code"}, 
        Property_Service_Codes_Custom, {"service_code"}, 
        "ChildServiceCodeInfo", JoinKind.LeftOuter),
    ExpandedChildServiceCodes = Table.ExpandTableColumn(
        JoinedChildServiceCodes, "ChildServiceCodeInfo", 
        {"is_active"}, {"child_is_active"}),
    
    // Filter to relevant services and add calculation columns
    FilteredServices = Table.SelectRows(ExpandedChildServiceCodes, each 
        ([parent_service_code] = null or 
         (([parent_service_status] = "C" or [parent_removal_date] > DateTime.LocalNow()) and
          (Text.Start([parent_service_code], 1) = "R" or 
           (Text.Start([parent_service_code], 1) = "G" and [parent_is_active] = "No") or 
           Text.Start([parent_service_code], 1) = "N"))) and
        ([child_service_code] = null or 
         (([child_service_status] = "C" or [child_removal_date] > DateTime.LocalNow()) and
          (Text.Start([child_service_code], 1) = "R" or 
           (Text.Start([child_service_code], 1) = "G" and [child_is_active] = "No") or 
           Text.Start([child_service_code], 1) = "N")))),
    
    // Add dummy columns for aggregation
    AddedParentServiceCount = Table.AddColumn(FilteredServices, "parent_service_count", each
        if [parent_service_code] <> null and [parent_service_code] <> "N0000UA" 
        then [parent_number_of_services] else 0, Int64.Type),
    AddedChildServiceCount = Table.AddColumn(AddedParentServiceCount, "child_service_count", each
        if [child_service_code] <> null and [child_service_code] <> "N0000UA" 
        then [child_number_of_services] else 0, Int64.Type),
    AddedParentRedCount = Table.AddColumn(AddedChildServiceCount, "parent_red_count", each
        if [parent_service_code] <> null and Text.Start([parent_service_code], 1) = "R" 
        then [parent_number_of_services] else 0, Int64.Type),
    AddedChildRedCount = Table.AddColumn(AddedParentRedCount, "child_red_count", each
        if [child_service_code] <> null and Text.Start([child_service_code], 1) = "R" 
        then [child_number_of_services] else 0, Int64.Type),
    AddedChildAnyService = Table.AddColumn(AddedChildRedCount, "child_any_service", each
        if [child_service_code] <> null then [child_number_of_services] else 0, Int64.Type),
    AddedParentAnyService = Table.AddColumn(AddedChildAnyService, "parent_any_service", each
        if [parent_service_code] <> null then [parent_number_of_services] else 0, Int64.Type),
    AddedN0000UACount = Table.AddColumn(AddedParentAnyService, "n0000ua_count", each
        if ([parent_service_code] = "N0000UA" and [parent_is_active] = "Yes") or 
           ([child_service_code] = "N0000UA" and [child_is_active] = "Yes") 
        then 1 else 0, Int64.Type),
    
    // Group by parent property with aggregations
    GroupedByParent = Table.Group(AddedN0000UACount, 
        {"property_key", "tpklpaprop", "formatted_address", "suburb"}, {
        {"total_number_of_services", each List.Sum([parent_service_count]) + List.Sum([child_service_count]), Int64.Type},
        {"number_of_red_services", each List.Sum([parent_red_count]) + List.Sum([child_red_count]), Int64.Type},
        {"child_services_total", each List.Sum([child_any_service]), Int64.Type},
        {"parent_services_total", each List.Sum([parent_any_service]), Int64.Type},
        {"has_n0000ua_count", each List.Sum([n0000ua_count]), Int64.Type},
        {"parent_service_active", each List.Min([parent_is_active]), Text.Type}
    }),
    
    // Add service category logic
    AddedServiceCategory = Table.AddColumn(GroupedByParent, "service_category", each
        if [child_services_total] > 0 then "Child Has Service"
        else if [parent_service_active] = null then "Missing Service"
        else if [parent_services_total] = 0 then "Zero Service"
        else if [parent_service_active] = "Yes" then "Has Service"
        else if [parent_service_active] = "No" then "Old Service Code"
        else "Unknown", Text.Type),
    
    AddedHasN0000UA = Table.AddColumn(AddedServiceCategory, "has_n0000ua", each
        if [has_n0000ua_count] > 0 then "Yes" else "No", Text.Type),
    
    // Clean up temporary columns
    FinalTable = Table.RemoveColumns(AddedHasN0000UA, 
        {"child_services_total", "parent_services_total", "has_n0000ua_count", "parent_service_active"})
in
    FinalTable

// MUDSpecialN0000UA
// MUDSpecialN0000UA
let
    Source = ChildProperties,
    
    // Filter to active child properties
    FilteredActive = Table.SelectRows(Source, each [status] = "C"),
    
    // Join parent property info to get parent TPKLPAPROP
    JoinedParentInfo = Table.NestedJoin(
        FilteredActive, {"parent_property_key"}, 
        MUDParentProperties, {"property_key"}, 
        "ParentInfo", JoinKind.Inner),
    ExpandedParentInfo = Table.ExpandTableColumn(
        JoinedParentInfo, "ParentInfo", 
        {"tpklpaprop"}, {"parent_tpklpaprop"}),
    
    // Join child services - filter to N0000UA only
    JoinedChildServices = Table.NestedJoin(
        ExpandedParentInfo, {"property_key"}, 
        Property_Services_Facts, {"property_key"}, 
        "ChildServices", JoinKind.Inner),
    ExpandedChildServices = Table.ExpandTableColumn(
        JoinedChildServices, "ChildServices", 
        {"service_code", "status"}, {"service_code", "service_status"}),
    FilteredN0000UA = Table.SelectRows(ExpandedChildServices, each 
        [service_code] = "N0000UA" and [service_status] = "C"),
    
    // Join child rates (must have rates)
    JoinedChildRates = Table.NestedJoin(
        FilteredN0000UA, {"property_key"}, 
        ChildRates, {"property_key"}, 
        "ChildRatesInfo", JoinKind.Inner),
    ExpandedChildRates = Table.ExpandTableColumn(
        JoinedChildRates, "ChildRatesInfo", 
        {"vgnumber", "ratepayer", "ratetype"}, {"vgnumber", "ratepayer", "ratetype"}),
    
    // Join child bin services - filter to manual calculation
    JoinedChildBinServices = Table.NestedJoin(
        ExpandedChildRates, {"property_key"}, 
        Reg_BSERV_Bin_Services, {"property_key_link_to_property"}, 
        "ChildBinServices", JoinKind.Inner),
    ExpandedChildBinServices = Table.ExpandTableColumn(
        JoinedChildBinServices, "ChildBinServices", 
        {"c_calculation_method", "c_audit_date"}, 
        {"child_calc_method", "audit_date"}),
    FilteredManualCalc = Table.SelectRows(ExpandedChildBinServices, each 
        Text.Start([child_calc_method], 3) = "MAN"),
    
    // Join parent services (should be null or no current services)
    JoinedParentServices = Table.NestedJoin(
        FilteredManualCalc, {"parent_property_key"}, 
        Property_Services_Facts, {"property_key"}, 
        "ParentServices", JoinKind.LeftOuter),
    ExpandedParentServices = Table.ExpandTableColumn(
        JoinedParentServices, "ParentServices", 
        {"service_code", "status"}, {"parent_service_code", "parent_service_status"}),
    FilteredNoParentServices = Table.SelectRows(ExpandedParentServices, each 
        [parent_service_code] = null or [parent_service_status] <> "C"),
    
    // Join parent bin services (should be null)
    JoinedParentBinServices = Table.NestedJoin(
        FilteredNoParentServices, {"parent_property_key"}, 
        Reg_BSERV_Bin_Services, {"property_key_link_to_property"}, 
        "ParentBinServices", JoinKind.LeftOuter),
    ExpandedParentBinServices = Table.ExpandTableColumn(
        JoinedParentBinServices, "ParentBinServices", 
        {"c_calculation_method"}, {"parent_calc_method"}),
    FilteredNoParentBinServices = Table.SelectRows(ExpandedParentBinServices, each 
        [parent_calc_method] = null),
    
    // Join parent rates (should be null)
    JoinedParentRates = Table.NestedJoin(
        FilteredNoParentBinServices, {"parent_property_key"}, 
        ParentRates, {"property_key"}, 
        "ParentRates", JoinKind.LeftOuter),
    ExpandedParentRates = Table.ExpandTableColumn(
        JoinedParentRates, "ParentRates", 
        {"ratetype"}, {"parent_ratetype"}),
    FilteredNoParentRates = Table.SelectRows(ExpandedParentRates, each 
        [parent_ratetype] = null),
    
    // Add constant columns and rename for final output
    AddedServiceCategory = Table.AddColumn(FilteredNoParentRates, "service_category", each "Child Has Service", Text.Type),
    AddedHasN0000UA = Table.AddColumn(AddedServiceCategory, "has_n0000ua", each "Yes", Text.Type),
    RenamedParentColumn = Table.RenameColumns(AddedHasN0000UA, {{"parent_tpklpaprop", "parent_tpklpaprop"}}),
    
    // Select final columns to match report structure
    FinalColumns = Table.SelectColumns(RenamedParentColumn, {
        "parent_tpklpaprop", "tpklpaprop", "formatted_address", "suburb", 
        "assessment", "vgnumber", "ratetype", "ratepayer", "audit_date", 
        "service_category", "has_n0000ua"})
in
    FinalColumns

// ParentRates
// ParentRates  
let
    Source = Rate_Type_Facts,
    // Join current DWM rate types
    JoinedRateTypes = Table.NestedJoin(
        Source, {"ratetype_key"}, 
        CurrentRateTypes, {"ratetype_key"}, 
        "RateTypeDetails", JoinKind.Inner),
    ExpandedRateTypes = Table.ExpandTableColumn(
        JoinedRateTypes, "RateTypeDetails", 
        {"is_current_period", "category_code", "ratetype"}, {"is_current_period", "category_code", "ratetype"}),
    
    // Filter to current DWM rates, exclude N0000UA
    FilteredRates = Table.SelectRows(ExpandedRateTypes, each 
        [is_current_period] = "Y" and 
        [category_code] = "DWM" and 
        [ratetype] <> "N0000UA" and
        ([suspended_date] = null or [suspended_date] > DateTime.LocalNow())),
    
    // Join assessment info
    JoinedAssessment = Table.NestedJoin(
        FilteredRates, {"assessment_key"}, 
        Rate_Assessment, {"assessment_key"}, 
        "AssessmentInfo", JoinKind.Inner),
    ExpandedAssessment = Table.ExpandTableColumn(
        JoinedAssessment, "AssessmentInfo", 
        {"assessment", "vgnumber", "ratepayer_address_1"}, 
        {"assessment", "vgnumber", "ratepayer"}),
    
    // Join to filter only parent properties
    JoinedParents = Table.NestedJoin(
        ExpandedAssessment, {"property_key"}, 
        MUDParentProperties, {"property_key"}, 
        "ParentInfo", JoinKind.Inner),
    
    SelectedColumns = Table.SelectColumns(JoinedParents, 
        {"property_key", "ratetype", "assessment", "vgnumber", "ratepayer"})
in
    SelectedColumns

// BinRatesWithoutServices
// BinRatesWithoutServices
let
    Source = Table.Combine({SUDAudit, MUDAudit, MUDSpecialN0000UA}),
    #"Changed Type" = Table.TransformColumnTypes(Source,{{"assessment", type text}, {"parent_tpklpaprop", Int64.Type}}),
    #"Renamed Columns" = Table.RenameColumns(#"Changed Type",{
        {"parent_tpklpaprop", "parent tpk"}, 
        {"tpklpaprop", "tpk"}, 
        {"formatted_address", "address"}, 
        {"ratetype", "rate type"}, 
        {"ratepayer", "rate payer"}, 
        {"audit_date", "audit date"}, 
        {"service_category", "service category"}, 
        {"has_n0000ua", "has n0000ua?"}
        })
in
    #"Renamed Columns"
