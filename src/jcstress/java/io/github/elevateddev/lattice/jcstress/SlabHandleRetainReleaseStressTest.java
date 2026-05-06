package io.github.elevateddev.lattice.jcstress;

import io.github.elevateddev.lattice.slab.SlabHandle;
import io.github.elevateddev.lattice.slab.SlabPool;
import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.Expect;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.IIII_Result;

@JCStressTest
@Outcome(id = "0, 0, 1, 1", expect = Expect.ACCEPTABLE,
    desc = "Release won; retain was rejected and the payload returned once.")
@Outcome(id = "1, 1, 0, 1", expect = Expect.ACCEPTABLE,
    desc = "Retain won; original release left one retained reference, then arbiter released it.")
@Outcome(expect = Expect.FORBIDDEN,
    desc = "Reference count must not leak, go negative, or release the pool permit twice.")
@State
public class SlabHandleRetainReleaseStressTest {

    private final SlabPool<String> pool = new SlabPool<>("jcstress-slab", 1);
    private final SlabHandle<String> handle = pool.acquire("payload");

    private SlabHandle<String> retained;
    private int retainResult;

    @Actor
    public void retain() {
        try {
            retained = handle.retain();
            retainResult = 1;
        } catch (final IllegalStateException ex) {
            retainResult = 0;
        }
    }

    @Actor
    public void releaseOriginal() {
        handle.release();
    }

    @Arbiter
    public void arbiter(final IIII_Result result) {
        result.r1 = retainResult;
        result.r2 = handle.references();
        result.r3 = (int) pool.releasedCount();
        final SlabHandle<String> retainedHandle = retained;
        if (retainedHandle != null) {
            retainedHandle.release();
        }
        result.r4 = (int) pool.releasedCount();
    }
}
