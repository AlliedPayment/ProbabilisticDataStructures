using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ProbabilisticDataStructures;
using StackExchange.Redis;
using TestProbabilisticDataStructures;


namespace TestProbabilisticDataStructures
{
    [TestClass]
    public class TestRedisBitfield
    {
        [TestMethod]
        public void TestRedisBitfieldSet()
        {
            uint offset = 0;
            byte length = 10;
            
            Buckets bools = new Buckets(1, 1);
            bools.Reset();

            Assert.AreEqual(bools.Get(0), 0);
            bools.Increment(0, 1);
            Assert.AreEqual(bools.Get(0), 1);


            Buckets test = new Buckets(2,length);
            test.SetBits(offset, length, 0);
            Assert.AreEqual(0, test.Get(offset));
            uint ret =test.SetBits(offset, length, 128);
            Assert.AreEqual(0, ret);
            ret =test.Get(offset);
            Assert.AreEqual(128, ret);

            test.Increment(offset,-1);
            Assert.AreEqual(test.Get(offset),ret -1);

            offset = 1;
            ret=test.Get(offset);
            test.Increment(offset, 1);
            Assert.AreEqual(ret+1, test.Get(offset));
        }

    }
}
