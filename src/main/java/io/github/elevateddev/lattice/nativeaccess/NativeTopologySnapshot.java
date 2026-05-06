package io.github.elevateddev.lattice.nativeaccess;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;

/**
 * Process-local native topology observations captured for graph and worker
 * startup. CPU topology is loaded lazily so graphs without placement do not pay
 * for a full JNI scan.
 */
public final class NativeTopologySnapshot {

    private static final int DEFAULT_SCAN_LIMIT = 1024;
    private static final CpuCounts NO_CPU_COUNTS = new CpuCounts(-1, -1, -1, 0);

    private final NativeProbe probe;
    private final boolean loaded;
    private final NativeCapabilities capabilities;
    private final Throwable failure;
    private final String failureMessage;
    private volatile int maxCpuCount;
    private volatile CpuCounts cpuCounts;
    private volatile CpuTopology cpuTopology;

    private NativeTopologySnapshot(
        final NativeProbe probe,
        final boolean loaded,
        final NativeCapabilities capabilities,
        final Throwable failure,
        final String failureMessage
    ) {
        this.probe = Objects.requireNonNull(probe, "probe");
        this.loaded = loaded;
        this.capabilities = capabilities;
        this.failure = failure;
        this.failureMessage = failureMessage == null ? "" : failureMessage;
        if (!loaded || capabilities == null) {
            this.maxCpuCount = -1;
            this.cpuCounts = NO_CPU_COUNTS;
            this.cpuTopology = CpuTopology.empty(NO_CPU_COUNTS);
        }
    }

    public static NativeTopologySnapshot captureSystem() {
        return capture(SystemNativeProbe.INSTANCE);
    }

    static NativeTopologySnapshot capture(final NativeProbe probe) {
        Objects.requireNonNull(probe, "probe");
        if (!probe.loaded()) {
            return new NativeTopologySnapshot(probe, false, null, null, probe.loadFailureMessage());
        }

        try {
            final NativeCapabilities capabilities = probe.capabilities();
            if (capabilities == null) {
                final NativeTopologyException failure =
                    new NativeTopologyException("native topology capabilities are unavailable");
                return new NativeTopologySnapshot(probe, true, null, failure, failure.getMessage());
            }
            return new NativeTopologySnapshot(probe, true, capabilities, null, "");
        } catch (final RuntimeException | UnsatisfiedLinkError ex) {
            return new NativeTopologySnapshot(probe, true, null, ex, throwableMessage(ex));
        }
    }

    public boolean loaded() {
        return loaded;
    }

    public boolean hasCapabilities() {
        return capabilities != null;
    }

    public NativeCapabilities capabilities() {
        if (capabilities == null) {
            throw new NativeTopologyUnavailableException(
                "native topology capabilities are unavailable: " + failureMessage,
                failure
            );
        }
        return capabilities;
    }

    public Throwable failure() {
        return failure;
    }

    public String failureMessage() {
        return failureMessage;
    }

    public CpuCounts cpuCounts() {
        CpuCounts current = cpuCounts;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            current = cpuCounts;
            if (current == null) {
                current = captureCpuCounts();
                cpuCounts = current;
            }
        }
        return current;
    }

    public int maxCpuCount() {
        int current = maxCpuCount;
        if (current != 0) {
            return current;
        }
        synchronized (this) {
            current = maxCpuCount;
            if (current == 0) {
                current = captureMaxCpuCount();
                maxCpuCount = current;
            }
        }
        return current;
    }

    public int configuredCpuCount() {
        return cpuCounts().configuredCpuCount();
    }

    public int onlineCpuCount() {
        return cpuCounts().onlineCpuCount();
    }

    public CpuTopology cpuTopology() {
        CpuTopology current = cpuTopology;
        if (current != null) {
            return current;
        }
        synchronized (this) {
            current = cpuTopology;
            if (current == null) {
                current = captureCpuTopology();
                cpuTopology = current;
            }
        }
        return current;
    }

    /**
     * Returns cached CPU-to-NUMA metadata only if a topology scan already ran.
     * This avoids turning a single worker observation into a full CPU scan.
     */
    public int cachedNumaNodeOfCpu(final int cpu) {
        final CpuTopology current = cpuTopology;
        return current == null ? -1 : current.numaNodeOfCpu(cpu);
    }

    public int numaNodeOfCpu(final int cpu) {
        return cpuTopology().numaNodeOfCpu(cpu);
    }

    private CpuCounts captureCpuCounts() {
        if (capabilities == null || (!capabilities.affinity() && !capabilities.numaQuery())) {
            return NO_CPU_COUNTS;
        }

        final int max = maxCpuCount();
        final int configured = positiveCount(max, probe::configuredCpuCount);
        final int online = positiveCount(configured, probe::onlineCpuCount);
        final int scanLimit = Math.max(1, Math.min(max, Math.max(configured, online)));
        return new CpuCounts(max, configured, online, scanLimit);
    }

    private int captureMaxCpuCount() {
        if (capabilities == null || (!capabilities.affinity() && !capabilities.numaQuery())) {
            return -1;
        }
        return positiveCount(DEFAULT_SCAN_LIMIT, probe::maxCpuCount);
    }

    private CpuTopology captureCpuTopology() {
        if (capabilities == null || !capabilities.affinity()) {
            return CpuTopology.empty(cpuCounts());
        }

        final CpuCounts counts = cpuCounts();
        if (counts.scanLimit() <= 0) {
            return CpuTopology.empty(counts);
        }

        final BitSet allowedCpus = new BitSet(counts.scanLimit());
        final int[] numaByCpu = new int[counts.scanLimit()];
        Arrays.fill(numaByCpu, -1);
        for (int cpu = 0; cpu < counts.scanLimit(); cpu++) {
            final boolean allowed;
            try {
                allowed = probe.isCpuAllowed(cpu);
            } catch (final RuntimeException | UnsatisfiedLinkError ex) {
                continue;
            }
            if (!allowed) {
                continue;
            }

            allowedCpus.set(cpu);
            if (!capabilities.numaQuery()) {
                continue;
            }
            try {
                numaByCpu[cpu] = probe.numaNodeOfCpu(cpu);
            } catch (final RuntimeException | UnsatisfiedLinkError ex) {
                // CPUs without NUMA metadata are still usable for CPU affinity.
            }
        }
        return new CpuTopology(counts, allowedCpus, numaByCpu);
    }

    private static int positiveCount(final int fallback, final NativeCount count) {
        try {
            final int value = count.get();
            return value > 0 ? value : fallback;
        } catch (final RuntimeException | UnsatisfiedLinkError ex) {
            return fallback;
        }
    }

    private static String throwableMessage(final Throwable failure) {
        final String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }

    public record CpuCounts(int maxCpuCount, int configuredCpuCount, int onlineCpuCount, int scanLimit) {
    }

    public static final class CpuTopology {
        private final CpuCounts counts;
        private final BitSet allowedCpus;
        private final int[] numaByCpu;
        private final boolean hasNumaMetadata;

        private CpuTopology(final CpuCounts counts, final BitSet allowedCpus, final int[] numaByCpu) {
            this.counts = Objects.requireNonNull(counts, "counts");
            this.allowedCpus = (BitSet) Objects.requireNonNull(allowedCpus, "allowedCpus").clone();
            this.numaByCpu = Objects.requireNonNull(numaByCpu, "numaByCpu").clone();
            this.hasNumaMetadata = hasNumaMetadata(this.numaByCpu);
        }

        private static CpuTopology empty(final CpuCounts counts) {
            return new CpuTopology(counts, new BitSet(), new int[0]);
        }

        public CpuCounts counts() {
            return counts;
        }

        public BitSet allowedCpus() {
            return (BitSet) allowedCpus.clone();
        }

        public int allowedCpuCount() {
            return allowedCpus.cardinality();
        }

        public boolean isCpuAllowed(final int cpu) {
            return cpu >= 0 && cpu < numaByCpu.length && allowedCpus.get(cpu);
        }

        public int numaNodeOfCpu(final int cpu) {
            if (cpu < 0 || cpu >= numaByCpu.length) {
                return -1;
            }
            return numaByCpu[cpu];
        }

        public boolean hasNumaMetadata() {
            return hasNumaMetadata;
        }

        private static boolean hasNumaMetadata(final int[] numaByCpu) {
            for (int i = 0; i < numaByCpu.length; i++) {
                if (numaByCpu[i] >= 0) {
                    return true;
                }
            }
            return false;
        }
    }

    @FunctionalInterface
    private interface NativeCount {
        int get();
    }

    interface NativeProbe {
        boolean loaded();

        String loadFailureMessage();

        NativeCapabilities capabilities();

        int maxCpuCount();

        int configuredCpuCount();

        int onlineCpuCount();

        boolean isCpuAllowed(int cpu);

        int numaNodeOfCpu(int cpu);
    }

    private enum SystemNativeProbe implements NativeProbe {
        INSTANCE;

        @Override
        public boolean loaded() {
            return NativeTopology.isLoaded();
        }

        @Override
        public String loadFailureMessage() {
            return NativeTopology.loadFailureMessage();
        }

        @Override
        public NativeCapabilities capabilities() {
            return NativeTopology.capabilities();
        }

        @Override
        public int maxCpuCount() {
            return NativeTopology.maxCpuCount();
        }

        @Override
        public int configuredCpuCount() {
            return NativeTopology.configuredCpuCount();
        }

        @Override
        public int onlineCpuCount() {
            return NativeTopology.onlineCpuCount();
        }

        @Override
        public boolean isCpuAllowed(final int cpu) {
            return NativeTopology.isCpuAllowed(cpu);
        }

        @Override
        public int numaNodeOfCpu(final int cpu) {
            return NativeTopology.numaNodeOfCpu(cpu);
        }
    }
}
