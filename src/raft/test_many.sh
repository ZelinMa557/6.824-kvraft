rm -f log*
for i in {1..100}
do
    go test -race -run 2A 1>log"$i"
    if [ $? -eq 0 ]; then
        rm -f log"$i"
    fi
    if [ $(($i % 10)) -eq 0 ]; then
        echo "test $i finish"
    fi
done