package ai.hocuspocus.core

import kotlin.coroutines.Continuation
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * The server resolves extension hooks by JVM method name, so a rename in
 * [HocuspocusExtension] that is not mirrored in [ExtensionHook] would silently
 * stop dispatching that hook instead of failing to compile. These tests pin
 * both directions of that mapping.
 */
class ExtensionHookCoverageTest {
    private val declaredHookMethodNames: Set<String> =
        HocuspocusExtension::class.java.declaredMethods
            .asSequence()
            .filter { method ->
                !method.isBridge &&
                    !method.isSynthetic &&
                    method.parameterCount == 2 &&
                    method.parameterTypes.last() == Continuation::class.java
            }
            .map { method -> method.name }
            .toSet()

    @Test
    fun `every dispatched hook name exists on the extension interface`() {
        val missing = ExtensionHook.entries
            .map(ExtensionHook::methodName)
            .filterNot { name -> name in declaredHookMethodNames }
        assertEquals(
            emptyList(),
            missing,
            "ExtensionHook names that no longer match a HocuspocusExtension method",
        )
    }

    @Test
    fun `every extension hook is dispatched by the server`() {
        val dispatched = ExtensionHook.entries.map(ExtensionHook::methodName).toSet()
        val undispatched = (declaredHookMethodNames - dispatched).sorted()
        assertEquals(
            emptyList(),
            undispatched,
            "HocuspocusExtension hooks with no ExtensionHook entry are never invoked",
        )
    }

    @Test
    fun `hook names are unique`() {
        val names = ExtensionHook.entries.map(ExtensionHook::methodName)
        assertEquals(names.size, names.toSet().size, "duplicate ExtensionHook method names")
    }
}
