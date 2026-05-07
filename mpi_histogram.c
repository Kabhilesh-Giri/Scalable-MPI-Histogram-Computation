/*
# NAME: Kabhilesh Giri
# NUID: 002371962
# NEU MAIL : giri.k@northeastern.edu
*/

#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <time.h>

#define NUM_ENTRIES 800000
#define MIN_VALUE 1
#define MAX_VALUE 100000

// The Structure Pair is used to create a data structure of pairs for addressing the ranges in an individual bins
struct pair
{
    int first; // Re-Implementation of C++ STL std::pair<>
    int second;
};

// This Data Structure contains all the information about the Individual Nodes running in the Distributed Systems
struct PROCESSOR
{
    int proc_part; // Holds the Rank of the Node
    int low; // The Lower Index of the dataset
    int high; // The Higher Index of the dataset
    int *local_dataset; // The Local Histogram that will be passed from the Root Node
    int size; // The Size of the Local Histogram
    struct pair* partition_bins; // This struct pointer stores the ranges inside the each bin
};

// This Function randomly generates values between 1 to 100000 and stores them into the dataset acc to the NUM_ENTRIES
void setDataSet(int *data_set)
{
    int random = 0;

    for (int i = 0; i < NUM_ENTRIES; i++)
    {
        data_set[i] = MIN_VALUE + rand() % (MAX_VALUE - MIN_VALUE + 1);
    }
}

/* This for loop is dedicatedly made to spilt the elements chunks for each processor so they can have dedicated chunks
 to operate with*/
void spiltFunction(int world_size, struct PROCESSOR* obj, int *data_set)
{
    int base_size = NUM_ENTRIES / world_size;
    int remainder = NUM_ENTRIES % world_size;

    for (int i = 0; i < world_size; i++) {
        obj[i].proc_part = i;

        obj[i].low = i * base_size + (i < remainder ? i : remainder);
        obj[i].high = obj[i].low + base_size - 1 + (i < remainder ? 1 : 0);

        obj[i].size = obj[i].high - obj[i].low + 1;

        int j = obj[i].low;

        // Allocating the array and intialize them to zero
        obj[i].local_dataset = (int *)malloc(obj[i].size * sizeof(int));

        // Allocate the range of values from the dataset to local_dataset
        for (int q = 0; q < obj[i].size; q++)
        {
            obj[i].local_dataset[q] = data_set[j];
            j++;
        }

        //Error handling to handle exception
        if (obj[i].local_dataset == NULL) {
            printf("malloc failed\n");
            exit(1);
        }
    }
}

int main(int argc, char**argv)
{
    srand(time(NULL)); // used to generate new random value each time

    MPI_Status status;
    int world_rank, world_size, bin_Count, i;

    if(argc < 2)
    {
        printf("Please Enter the number of bins inside the Sbatch Script\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 1); // Abort if no argument provided
    }

    bin_Count = atoi(argv[1]); // Convert command-line input to integer

    // Since the Dataset is significantly larger the Stack size won't be suffice so the dynamically memory allocation is adopted
    int *data_set = (int*)malloc(NUM_ENTRIES*sizeof(int));

    MPI_Init(&argc,&argv); // To Start the MPI Program Segment
    MPI_Comm_size(MPI_COMM_WORLD, &world_size); // Get total number of processes
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank); // Get process rank

    int bin_low = 0, bin_high = 0;
    int bin_Width = MAX_VALUE / bin_Count; // The Range of each bin

    struct pair bin[bin_Count]; // Create the number of Bins

    // To Initialize the Bin Specification
    for (int f = 0; f < bin_Count; f++)
    {
        bin_low = f * bin_Width + 1;
        bin_high = bin_low + bin_Width - 1;

        if(f == bin_Count - 1)
        {
            bin_high = MAX_VALUE;
        }

        bin[f].first = bin_low;
        bin[f].second = bin_high;
    }

    if(world_rank == 0) // Only root Node will create the arrays // 0
    {
        double start_time, end_time; // To calculate the performance of the program
        struct PROCESSOR obj[world_size];
        setDataSet(data_set);

        //Now that I have processed the dataset into local partitions for the number of ranks
        spiltFunction(world_size,obj,data_set);


        start_time = MPI_Wtime(); // Timer Calculate
        int size = obj[0].size;

        // The Calloc is used to avoid the garbage allocation which can make the system behaviour in undefined manner
        int *local_histogram = (int*)calloc(bin_Count, sizeof(int));
        int *final_histogram = (int*)calloc(bin_Count,sizeof(int));

        // Intialize the Local Histogram for each nodes
        for (int f = 0; f < bin_Count; f++)
        {
            for (int i = 0; i < size; i++)
            {
                if (obj[0].local_dataset[i] >= bin[f].first && obj[0].local_dataset[i] <= bin[f].second)
                {
                    local_histogram[f]++;
                }
            }
        }

//////////////////////This section of the code is solely for debugging purpose//////////////////////

    // int total_local_count = 0;

    // printf("World Rank : %d size of local data : %d\n",world_rank,size);
    // for (int i = 0; i < bin_Count; i++) {

    // printf("Rank %d - Bin[%d] (%d - %d) = %d\n", world_rank, i, bin[i].first, bin[i].second, local_histogram[i]);
    // total_local_count += local_histogram[i];
    // }

    // printf("Rank %d - Testing whether any data loss occured Local Count = %d\n", world_rank, total_local_count);
    // printf("\n");

////////////////////////////////////////////////////////////////////////////////////////////////////

        //I have to broadcast the local_dataset to each ranks to process the data
        //Since in MPI we can only send/broacast individual values I have used multiple MPI_send to broadcast the data
        for (int i = 1; i < world_size; i++)
        {
            MPI_Send(&obj[i].low, 1 ,MPI_INT, i, 0, MPI_COMM_WORLD);
            MPI_Send(&obj[i].high, 1 ,MPI_INT, i, 1, MPI_COMM_WORLD);
            MPI_Send(obj[i].local_dataset, obj[i].size, MPI_INT, i, 2, MPI_COMM_WORLD);
        }

        // To receive the values from the each node and use MPI_SUM to add up all the corresponding values
        MPI_Reduce(local_histogram,final_histogram,bin_Count,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD);

        int total = 0;
        printf("Final histogram :\n");
        for (int i = 0; i < bin_Count; i++) {
            printf("Bin[%d] (%d - %d) = %d\n",
                i, bin[i].first, bin[i].second, final_histogram[i]);
                total += final_histogram[i];
            }
        printf("Final Node Rank %d - Testing whether any data loss occured Local Count = %d\n", world_rank, total);
        printf("\n");

        free(final_histogram);
        free(local_histogram);
        for (int i = 0; i < world_size; i++) {
            free(obj[i].local_dataset);
        }

        end_time = MPI_Wtime();

        printf("Total Time Consumed : %f\nBins Count : %d\nNumber of Distributed Systems Involved : %d\n",
        (end_time-start_time),bin_Count,world_size);
    }
    else // All the Other Ranks will come here // 1 only node 2 will receive
    {
        int low, high;

        // Recv end from the root node to get the intricacies of the local histogram
        MPI_Recv(&low,1, MPI_INT, 0, 0, MPI_COMM_WORLD, &status);
        MPI_Recv(&high,1, MPI_INT, 0, 1, MPI_COMM_WORLD, &status);

        int size = high - low + 1;
        int *local_data = (int*)malloc(size*sizeof(int));

        MPI_Recv(local_data, size, MPI_INT,0, 2,MPI_COMM_WORLD, &status);

        int *local_histogram = (int*)calloc(bin_Count, sizeof(int));

        // Compute the Local Histogram
        for (int f = 0; f < bin_Count; f++)
        {
            for (int i = 0; i < size; i++)
            {
                if (local_data[i] >= bin[f].first && local_data[i] <= bin[f].second) {
                    local_histogram[f]++;
                }
            }
        }

//////////////////////This section of the code is solely for debugging purpose//////////////////////

    // int total_local_count = 0;

    // printf("World Rank : %d size of local data : %d\n",world_rank,size);
    // for (int i = 0; i < bin_Count; i++) {

    // printf("Rank %d - Bin[%d] (%d - %d) = %d\n", world_rank, i, bin[i].first, bin[i].second, local_histogram[i]);
    // total_local_count += local_histogram[i];
    // }
    // printf("Rank %d - Testing whether any data loss occured Local Count = %d\n", world_rank, total_local_count);
    // printf("\n");

////////////////////////////////////////////////////////////////////////////////////////////////////

    /*This Reduce statement actually sends back the computed value of histogram in an single array
    from the other nodes to the root node*/
    MPI_Reduce(local_histogram,NULL,bin_Count,MPI_INT,MPI_SUM,0,MPI_COMM_WORLD);

    free(local_histogram);
    free(local_data);
    }

    free(data_set);

    MPI_Finalize(); // End of the Distributed Systems

    return 0;
}
