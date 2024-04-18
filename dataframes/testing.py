def selectionsort(arr):
    n = len(arr)-1

    for i in range(n):
        min_num = i

        for j in range(i+1,n):
            if arr[min_num] > arr[j]:
                min_num = j
        arr[i], arr[min_num] = arr[min_num], arr[i]



