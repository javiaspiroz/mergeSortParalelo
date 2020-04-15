from multiprocessing import Process, Pipe
import time, random, sys

def merge(left, right):#Devuelvo las sublistas ordenadas
    sol = []
    li = ri = 0
    while li < len(left) and ri < len(right):
        if left[li] <= right[ri]:
            sol.append(left[li])
            li += 1
        else:
            sol.append(right[ri])
            ri += 1
    if li == len(left):
        sol.extend(right[ri:])
    else:
        sol.extend(left[li:])
    return sol

def mergeSort(arr): #Devuelvo una copia de la lista ordenada
    if len(arr) <= 1:
        return arr
    med = len(arr)//2
    return merge(mergeSort(arr[:med]), mergeSort(arr[med:]))

def mergeSortParalelo(arr, conn, procNum): #mergeSort paralelo
    if procNum <= 0 or len(arr) <= 1:#Caso base
        conn.send(mergeSort(arr))
        conn.close()
        return

    med = len(arr)//2

    #Creamos los procesos para ordenar ambas partes de la lista
    pconnLeft, cconnLeft = Pipe()
    leftProc = Process(target=mergeSortParalelo, \
                       args=(arr[:med], cconnLeft, procNum - 1))

    pconnRight, cconnRight = Pipe()
    rightProc = Process(target=mergeSortParalelo, \
                       args=(arr[med:], cconnRight, procNum - 1))

    #Lanzamos los subprocesos
    leftProc.start()
    rightProc.start()

    #Mezclamos las listas ordenadas
    conn.send(merge(pconnLeft.recv(), pconnRight.recv()))
    conn.close()

    #Unimos ambos subprocesos
    leftProc.join()
    rightProc.join()

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
    inicioP = time.time()
    n = 3 #2**(n+1) - 1 procesos que se instanciarán

    #Instanciamos un proceso con la lista completa
    pconn, cconn = Pipe()
    p = Process(target=mergeSortParalelo, \
                args=(arr, cconn, n))
    p.start()
    arr = pconn.recv() #Esperamos a recibir la lista ordenada
    
    p.join()
    finP = time.time()
    print('\nLa ejecución de MergeSort en PARALELO ha tardado ', finP-inicioP)