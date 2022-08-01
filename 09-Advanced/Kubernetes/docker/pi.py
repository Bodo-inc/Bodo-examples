"""
Monte Carlo Pi Calculation example.
    Usage: mpiexec -n [cores] python pi.py --points [points]
Similar to Spark's Pi example:
https://github.com/apache/spark/blob/master/examples/src/main/python/pi.py
"""
import bodo
import numpy as np
import argparse
import time


@bodo.jit
def calc_pi(n):
    t1 = time.time()
    x = 2 * np.random.ranf(n) - 1
    y = 2 * np.random.ranf(n) - 1
    pi = 4 * np.sum(x ** 2 + y ** 2 < 1) / n
    print("Execution time:", time.time() - t1, "\nresult:", pi)
    return pi


def main():
    parser = argparse.ArgumentParser(description="Monte Carlo Pi Calculation.")
    parser.add_argument("--points", dest="points", type=int, default=200000000)
    args = parser.parse_args()
    points = args.points
    calc_pi(points)


if __name__ == "__main__":
    main()
    