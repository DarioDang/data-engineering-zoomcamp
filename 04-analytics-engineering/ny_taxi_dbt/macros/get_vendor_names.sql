{% macro get_vendor_names(vendor_id) -%}
case
  when cast({{ vendor_id }} as int64) = 1 then 'Creative Mobile Technologies, LLC'
  when cast({{ vendor_id }} as int64) = 2 then 'VeriFone Inc.'
  else 'Unknown'
end
{%- endmacro %}