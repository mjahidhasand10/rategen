// Package benchmarks compares rategen with established Go rate limiters on
// equivalent workloads. It is a separate module so that the libraries it
// compares against never become dependencies of rategen.
//
// Every comparison uses the same algorithm family and parameters on both
// sides, and measures the admit path (limits set high enough that every
// request is admitted) unless the benchmark name says otherwise. See
// docs/benchmarks.md for methodology and results.
package benchmarks
