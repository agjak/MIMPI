#include <assert.h>
#include <stdio.h>

#include "mimpi_err.h"
#include "../mimpi.h"

int main() {
    MIMPI_Init(false);
    int rank = MIMPI_World_rank();
    printf("1 %d\n",rank);
    int a = 1, b = 2;
    if (rank == 0) {
        printf("2 %d\n",rank);
        ASSERT_MIMPI_OK(MIMPI_Send(&a, sizeof(int), 1, 1));
        ASSERT_MIMPI_OK(MIMPI_Send(&b, sizeof(int), 1, 2));

        ASSERT_MIMPI_OK(MIMPI_Send(&a, sizeof(int), 2, 1));
        ASSERT_MIMPI_OK(MIMPI_Send(&b, sizeof(int), 2, 2));

        ASSERT_MIMPI_OK(MIMPI_Send(&a, sizeof(int), 3, 1));
        ASSERT_MIMPI_OK(MIMPI_Send(&b, sizeof(int), 3, 2));
        printf("3 %d\n",rank);
    } if (rank == 1) {
        printf("2 %d\n",rank);
        ASSERT_MIMPI_OK(MIMPI_Recv(&a, sizeof(int), 0, 2));
        printf("2a %d\n",rank);
        assert(a == 2);
        printf("3 %d\n",rank);
    } else if (rank == 2) {
        printf("2 %d\n",rank);
        ASSERT_MIMPI_OK(MIMPI_Recv(&b, sizeof(int), 0, 1));
        printf("2a %d\n",rank);
        assert(b == 1);
        printf("3 %d\n",rank);
    } else if (rank == 3) {
        printf("2 %d\n",rank);
        ASSERT_MIMPI_OK(MIMPI_Recv(&b, sizeof(int), 0, MIMPI_ANY_TAG));
        printf("2a %d\n",rank);
        assert(b == 1);
        printf("3 %d\n",rank);
    }
    printf("Done!\n");
    MIMPI_Finalize();
    printf("4 %d\n",rank);
}