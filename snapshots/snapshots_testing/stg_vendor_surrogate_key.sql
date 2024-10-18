select {{ dbt_utils.generate_surrogate_key(['VNDR_ID', 'ADDR_OWNER', 'ADDR_FUNCT_DP', 'ADDR_LN_1', 'ADDR_LN_2', 'CITY', 'ST_CD', 'ZIP_CD', 'CTRY_CD']) }},
*
 from {{ source('daimler_mock', 'vendor_address') }}




