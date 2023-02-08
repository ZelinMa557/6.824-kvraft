rm -f log*
for i in {1..500}
do
    go test -race -run 2 1>log_2_"$i"
    if [ $? -eq 0 ]; then
        rm -f log_2_"$i"
    fi
    if [ $(($i % 10)) -eq 0 ]; then
        echo "test 2 $i finish"
    fi
done