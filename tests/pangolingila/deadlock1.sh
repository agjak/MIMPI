set -e
for i in {1..100} ; do
    echo 1
    ./run_test 1 2 examples_build/deadlock1
    echo 2
    ./run_test 1 2 examples_build/deadlock2
    echo 3
    ./run_test 1 2 examples_build/deadlock3
    echo 4
    ./run_test 1 3 examples_build/deadlock4
    echo 5
    ./run_test 1 2 examples_build/deadlock5
    echo 6
    ./run_test 5 2 examples_build/deadlock6
done
echo "done"