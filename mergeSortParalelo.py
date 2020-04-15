import random #Para generar números aleatorios
import math
import multiprocessing as mp # Para trabajar en paralelo
import time

def mergeSort(arr):
    if len(arr) <= 1: # Caso base
        return arr

    med = int(len(arr) / 2) #Obtenemos el punto medio del array
    #Volvemos a invocar con los arrays reducidos
    left, right = mergeSort(arr[:med]), mergeSort(arr[med:])

    return merge(left, right)


def merge(*args):
    # Argumentos usados
    left, right = args[0] if len(args) == 1 else args

    sol = [] #Array ordenado a rellenar
    li = ri = 0 #Índices de posicionamiento

    # Bucle para ordenar el array
    while li < len(left) and ri < len(right):
        if left[li] < right[ri]:
            sol.append(left[li])
            li += 1

        else:
            sol.append(right[ri])
            ri += 1

    sol.extend(left[li:])
    sol.extend(right[ri:])

    return sol

def mergeSortParalelo(data):
    n_cores = mp.cpu_count() #Número de cores del PC
    pool = mp.Pool(processes=n_cores) #Pool con los procesos a usar
    size = int(math.ceil(float(len(data)) / n_cores)) #Dividimos el array en partes de igual tamaño
    data = [data[i * size:(i + 1) * size] for i in range(n_cores)]
    data = pool.map(mergeSort, data) #Invocamos a mergeSort

    # Unimos las partes usando el pool
    while len(data) > 1:
        if len(data) % 2 == 1:
            aux = data.pop() 
        else: 
            aux = None
        
        data = [(data[i], data[i + 1]) for i in range(0, len(data), 2)]
        data = pool.map(merge, data) + ([aux] if aux else [])

    return data[0]

if __name__ == '__main__': 
    #Creo array de tamaño num expediente que contiene nums aleatorios con un rango entre -99999 y 99999
    arrIni = [random.randint(-99999, 99999) for i in range(21957644)]

    #Mergesort Secuencial
    arr = list(arrIni)
    inicioS = time.time()
    arr = mergeSort(arr)
    finS = time.time()
    print('La ejecución de MergeSort Secuencial ha tardado ', finS-inicioS)

    time.sleep(3) #Dejamos descansar al PC

    #Mergesort Paralelo
    arr = list(arrIni)
    iniP = time.time()
    mergeSortParalelo(arr) #Invoco el método mergeSort
    finP = time.time()
    print('\nLa ejecución de MergeSort en PARALELO ha tardado ', finP-iniP)