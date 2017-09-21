
wait_for_line () {
    while read line
    do
        echo "$line" | grep -q "$1" && break
        echo "$line" | grep "$2" && exit 1
    done < "$3"
    # Read the fifo for ever otherwise process would block
    cat "$3" >/dev/null &
}

function clean_exit(){
    local error_code="$?"
    for job in `jobs -p`
    do
        kill -9 $job
    done
    rm -rf "$1"
    return $error_code
}


