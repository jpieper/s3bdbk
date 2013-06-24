#!/usr/bin/python

# Copyright 2013 Josh Pieper, jjp@pobox.com

import datetime
import unittest

import s3bdbk

class TestCase(unittest.TestCase):
    def setUp(self):
        pass

    def sample_manifests(self, manifests, count):
        # This is a random process.  Take a bunch of samples and
        # measure the histogram.
        results = [0] * len(manifests)

        for i in range(count):
            this_result = s3bdbk.select_manifest_to_remove(manifests)
            index = manifests.index(this_result)
            results[index] += 1

        return results

    def test_select_manifest(self):
        manifests = [
            'manifest-20130101-162401-stuff',
            'manifest-20130102-162401-stuff',
            'manifest-20130103-162401-stuff',
            'manifest-20130104-162401-stuff',
            'manifest-20130105-162401-stuff',
            'manifest-20130106-162401-stuff',
            'manifest-20130107-162401-stuff',
            'manifest-20130108-162401-stuff',
            ]

        count = 5000

        results = self.sample_manifests(manifests, count)

        # Ensure that the first and last are never picked.  Also
        # ensure that the remainder are picked to be relatively evenly
        # spaced.
        self.assertEqual(results[0], 0)
        self.assertEqual(results[-1], 0)

        expected = count / (len(results) - 2)
        for result in results[1:-1]:
            self.assertTrue(abs(result - expected) < 0.8 * expected)

        # If we we now remove one of the items, we expect its neighbor
        # to be selected less frequently.
        del manifests[4]

        results = self.sample_manifests(manifests, count)
        expected = count / (len(results) - 2)
        self.assertTrue(results[4] < 0.8 * expected)

    def test_select_manifest_over_time(self):
        manifests = []

        current = datetime.datetime(2013, 1, 1, 6, 0, 0)

        for i in range(200):
            manifests.append('manifest-%04d%02d%02d-060000-stuff' % (
                current.year, current.month, current.day))

            current = current + datetime.timedelta(1)

            if len(manifests) > 25:
                del manifests[
                    manifests.index(
                        s3bdbk.select_manifest_to_remove(manifests))]

        # TODO: Construct some assertion that this is doing the right
        # thing.
                
        #print '\n'.join(manifests)

if __name__ == '__main__':
    unittest.main()
    
