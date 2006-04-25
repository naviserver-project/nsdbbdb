set db [ns_db gethandle berkeleydb]

# Create records
ns_db exec $db "PUT key1\ndata1"
ns_db exec $db "PUT key2\ndata2"
ns_db exec $db "PUT key3\ndata3"
ns_db exec $db "PUT key4\ndata4"
ns_db exec $db "PUT key5\ndata5"

# Retrieve one record
set query [ns_db 0or1row $db "GET key1"]
set result [ns_set value $query 0]
ns_log Debug GET: $result

# Delete one record
ns_db exec $db "DEL key1"

# Retrieve all records
set result ""
set query [ns_db select $db "CURSOR"]
while { [ns_db getrow $db $query] } {
  set row ""
  for { set i 0 } { $i < [ns_set size $query] } { incr i } {
    lappend row [ns_set value $query $i]
  }
  lappend result $row
}
ns_log Debug CURSOR: $result

ns_db releasehandle $db
