import os
import sys
import importlib
import doctest
import time

class TimedOutputChecker(doctest.OutputChecker):
    def __init__(self):
        self.timings = []

    def check_output(self, want, got, optionflags):
        start_time = time.time()
        result = super().check_output(want, got, optionflags)
        end_time = time.time()
        self.timings.append(end_time - start_time)
        return result

class TimedDocTestRunner(doctest.DocTestRunner):
    def __init__(self, *args, **kwargs):
        self.checker = TimedOutputChecker()
        self.test_names = []
        self.test_counts = []
        self.total_failures = 0
        super().__init__(checker=self.checker, *args, **kwargs)

    def run(self, test, compileflags=None, out=None, clear_globs=True):
        self.test_names.append(test.name)
        num_tests = len(test.examples)
        self.test_counts.append(num_tests)
        result = super().run(test, compileflags, out, clear_globs)
        self.total_failures += result.failed
        return result

    def summarize(self, verbose=None):
        #total_tests = sum(self.test_counts)
        failed_tests = self.total_failures
        #passed_tests = total_tests - failed_tests

        #print(f"Total tests run: {total_tests}")
        #print(f"Tests passed: {passed_tests}")
        print(f"Tests failed: {failed_tests}")

        test_index = 0
        for i, test_name in enumerate(self.test_names):
            num_tests = self.test_counts[i]
            total_time = sum(self.checker.timings[test_index:test_index + num_tests])
            print(f"{test_name} total time taken: {total_time:.4f} seconds")
            test_index += num_tests

        super().summarize(verbose)

total_tests_discovered = 0


def run_test(module_name, test_name=None, runner=None):
    global total_tests_discovered
    try:
        module = importlib.import_module(module_name)
        optionflags = doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE
        if runner is None:
            runner = TimedDocTestRunner(optionflags=optionflags)
        if test_name: 
            tests = doctest.DocTestFinder().find(module, name=test_name)
            if tests:
                print(f"Running {len(tests)} tests for {module_name}.{test_name}")
                total_tests_discovered += len(tests)
                for test in tests:
                    runner.run(test)
            else:
                print(f"No test named {test_name} found in {module_name}")
        else:
            tests = doctest.DocTestFinder().find(module)
            #print(f"Discovered {len(tests)} test objects in {module_name}")
            total_tests_discovered += len(tests)
            for test in tests:
                runner.run(test)
        print(f"Total tests discovered so far: {total_tests_discovered}")
        return runner
    except ImportError as e:
        print(f"Error importing {module_name}: {e}")
        return runner

def discover_and_run_tests():
    runner = TimedDocTestRunner(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    for root, _, files in os.walk('.'):
        for file in files:
            if file.endswith('.py') and file != 'test_runner.py' and file != '__init__.py':
                module_name = os.path.splitext(file)[0]
                runner = run_test(module_name, runner=runner)
    runner.summarize()

if __name__ == "__main__":
    # Add the current directory to the Python path
    sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

    discover_and_run_tests()

