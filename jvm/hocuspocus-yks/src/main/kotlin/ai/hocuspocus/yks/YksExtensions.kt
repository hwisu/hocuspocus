package ai.hocuspocus.yks

import ai.hocuspocus.core.DirectConnection
import ai.hocuspocus.core.HocuspocusDocument
import dev.yks.YDoc

public suspend fun <C : Any> HocuspocusDocument<C>.transactYks(
    context: C? = null,
    skipStoreHooks: Boolean = false,
    mutation: (YDoc) -> Unit,
) {
    transact(YDoc::class, context, skipStoreHooks, mutation)
}

public suspend fun <C : Any> DirectConnection<C>.transactYks(
    skipStoreHooks: Boolean = false,
    mutation: (YDoc) -> Unit,
) {
    transact(YDoc::class, skipStoreHooks, mutation)
}
