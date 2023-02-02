rm -f log*
# for i in {1..100}
# do
#     go test -race -run 2A 1>log_2A_"$i"
#     if [ $? -eq 0 ]; then
#         rm -f log_2A_"$i"
#     fi
#     if [ $(($i % 10)) -eq 0 ]; then
#         echo "test 2A $i finish"
#     fi
# done

for i in {1..500}
do
    go test -race -run 2B 1>log_2B_"$i"
    if [ $? -eq 0 ]; then
        rm -f log_2B_"$i"
    fi
    if [ $(($i % 10)) -eq 0 ]; then
        echo "test 2B $i finish"
    fi
done