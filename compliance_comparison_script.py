def truncate(words, count):
    words = list(words)
    if count == None:
        count = int(len(words) / 2)
    return words[:count]

with open('compliance_outputs.txt') as fin:
    tests = set()
    for index, line in enumerate(fin):
        segments = line.split('::')
        if len(segments) == 1:
            continue
        else:
            test_segment = segments[2]
            if " <- " in test_segment:
                test, results = test_segment.split(" <- ")
            else:
                test, *results = test_segment.split()
  
            if "[" in test:
                test, remainder = test.split("[")

            print(index, test, results)
            if 'ERROR' in results:
                break
            else:
                tests.add(test)

    count = 258
    tests = truncate(tests, count)
    keywords = ' or '.join(tests) + ' or test_contains_autoescape'
    print(keywords, count)