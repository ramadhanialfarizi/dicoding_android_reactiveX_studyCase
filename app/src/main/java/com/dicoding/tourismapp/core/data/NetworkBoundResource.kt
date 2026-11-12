package com.dicoding.tourismapp.core.data

import com.dicoding.tourismapp.core.data.source.remote.network.ApiResponse
import io.reactivex.rxjava3.android.schedulers.AndroidSchedulers
import io.reactivex.rxjava3.core.Flowable
import io.reactivex.rxjava3.schedulers.Schedulers

abstract class NetworkBoundResource<ResultType : Any, RequestType> {

    private var result: Flowable<Resource<ResultType>>

    init {
        @Suppress("LeakingThis")
        val dbSource = loadFromDB()

        result = dbSource.take(1).flatMap { data ->
            if (shouldFetch(data)) {
                createCall()
                    .subscribeOn(Schedulers.io())
                    .flatMap { apiResponse ->
                        when (apiResponse) {
                            is ApiResponse.Success -> {
                                saveCallResult(apiResponse.data)
                                // Terus dengarkan perubahan database sebagai Single Source of Truth
                                loadFromDB().map { Resource.Success(it) as Resource<ResultType> }
                            }
                            is ApiResponse.Empty -> {
                                loadFromDB().map { Resource.Success(it) as Resource<ResultType> }
                            }
                            is ApiResponse.Error -> {
                                onFetchFailed()
                                loadFromDB().map { Resource.Error(apiResponse.errorMessage, it) as Resource<ResultType> }
                            }
                        }
                    }
                    .startWithItem(Resource.Loading(data)) // Mengirim loading state sebelum network request
                    .observeOn(AndroidSchedulers.mainThread())
            } else {
                dbSource.map { Resource.Success(it) as Resource<ResultType> }
            }
        }
    }

    protected open fun onFetchFailed() {}

    protected abstract fun loadFromDB(): Flowable<ResultType>

    protected abstract fun shouldFetch(data: ResultType?): Boolean

    protected abstract fun createCall(): Flowable<ApiResponse<RequestType>>

    protected abstract fun saveCallResult(data: RequestType)

    fun asFlowable(): Flowable<Resource<ResultType>> = result
}
