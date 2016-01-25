var Promise     = require('promise');
var proxyquire  = require('proxyquire');
var sinon       = require('sinon');
//require('sinon-as-promised');

var chai        = require('chai');
var sinonChai   = require('sinon-chai');
var chaiAsPromised = require('chai-as-promised');

var should = chai.should();
var expect = chai.expect;
chai.use(sinonChai);
chai.use(chaiAsPromised);

var zk          = require('../lib/zkClient.js');

describe('lib/zookeeper test', function() {
    var loggerStub;
    var cacheStub;
    var configStub;
    var libZookeeper;

    var zkStub ;

    beforeEach(function() {
        loggerStub      = sinon.stub();
        cacheStub       = {get : sinon.stub(), set : sinon.stub() };
        configStub      = sinon.stub();
        zkStub          = { getChildren : sinon.stub(), getData : sinon.stub() };

        libZookeeper    = proxyquire(
            '../lib/zookeeper.js', {
                './Cache.js' : cacheStub,
                '../config.js': configStub,
                '../logger.js': { logger : { info: sinon.stub(), debug: sinon.stub(), trace: sinon.stub(), warn: sinon.stub(), error: sinon.stub() } },
                './zkClient.js' : zkStub
            }
        );
    });

    afterEach(function(){
        zkStub.getChildren.reset();
        zkStub.getData.reset();

        cacheStub.get.reset();
        cacheStub.set.reset();
    });

    describe('getting consumers', function() {

var getChildrenStub;
var getDataStub;
        beforeEach(function() {
            getChildrenStub = zkStub.getChildren;
            getDataStub     = zkStub.getData;
        });

        function testResult(expected, actual) {
            expect(actual).to.deep.equal(expected);
            expect(cacheStub.set.callCount).to.equal(expected.length);

            for(var count = 0; count < expected.length; count++) {
                var consumer = expected[count];
                expect(cacheStub.set.getCall(count).args[0]).to.deep.equal(consumer[0].consumer);
                expect(cacheStub.set.getCall(count).args[1]).to.deep.equal(consumer);
            }
        }

        it('returns consumer with no topics registered', function() {
            getChildrenStub.withArgs('/consumers').returns(Promise.resolve({data:['group1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets').returns(Promise.reject('testing no topics in a group'));

            var expectedResult = [ [
               { consumer: 'group1', topic: '', partition: '' }
            ]];

            return libZookeeper.getConsumers().then(function(result){
                testResult(expectedResult, result);
            });
        });

        it('returns consumer with a topic but no partitions registered', function() {
            getChildrenStub.withArgs('/consumers').returns(Promise.resolve({data:['group1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets').returns(Promise.resolve({data:['topic1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets/topic1').returns(Promise.resolve({data:[]}));

            var expectedResult = [ [
               { consumer: 'group1', topic: 'topic1', partition: '' }
           ] ];

            return libZookeeper.getConsumers().then(function(result){
                testResult(expectedResult, result);
            });
        });


        it('returns consumer with a topic and partition but no offsets registered', function() {
            getChildrenStub.withArgs('/consumers').returns(Promise.resolve({data:['group1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets').returns(Promise.resolve({data:['topic1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets/topic1').returns(Promise.resolve({data:['partition0']}));
            getDataStub.withArgs('/consumers/group1/offsets/topic1/partition0').returns(Promise.reject('testing no commited offset'));

            var expectedResult = [ [
               { consumer: 'group1', topic: 'topic1', partition: 'partition0', offset: undefined }
           ] ];

            return libZookeeper.getConsumers().then(function(result){
                testResult(expectedResult, result);
            });
        });


        it('returns consumer with a topic and partition but no owner registered', function() {
            getChildrenStub.withArgs('/consumers').returns(Promise.resolve({data:['group1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets').returns(Promise.resolve({data:['topic1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets/topic1').returns(Promise.resolve({data:['partition0']}));

            getDataStub.withArgs('/consumers/group1/offsets/topic1/partition0').returns(Promise.resolve({ data : 100 }));
            getDataStub.withArgs('/consumers/group1/owners/topic1/partition0').returns(Promise.reject('testing no registered owner'));

            var expectedResult = [ [
               { consumer: 'group1', topic: 'topic1', partition: 'partition0', offset: '100', owner: undefined }
           ] ];

            return libZookeeper.getConsumers().then(function(result){
                testResult(expectedResult, result);
            });
        });


        it('returns consumer with a topic and partition but no owner registered', function() {
            getChildrenStub.withArgs('/consumers').returns(Promise.resolve({data:['group1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets').returns(Promise.resolve({data:['topic1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets/topic1').returns(Promise.resolve({data:['partition0']}));

            getDataStub.withArgs('/consumers/group1/offsets/topic1/partition0').returns(Promise.resolve({ data : 100 }));
            getDataStub.withArgs('/consumers/group1/owners/topic1/partition0').returns(Promise.resolve({ data : 'owner1' }));

            var expectedResult = [ [
               { consumer: 'group1', topic: 'topic1', partition: 'partition0', offset: '100', owner: 'owner1' }
           ] ];

            return libZookeeper.getConsumers().then(function(result){
                testResult(expectedResult, result);
            });
        });


        it('returns multiple consumers correctly', function() {
            getChildrenStub.withArgs('/consumers').returns(Promise.resolve({data:['group1', 'group2']}));

            getChildrenStub.withArgs('/consumers/group1/offsets').returns(Promise.resolve({data:['topic1']}));
            getChildrenStub.withArgs('/consumers/group1/offsets/topic1').returns(Promise.resolve({data:['partition0', 'partition1']}));
            getChildrenStub.withArgs('/consumers/group2/offsets').returns(Promise.reject('testing no topics in a group'));

            getDataStub.withArgs('/consumers/group1/offsets/topic1/partition0').returns(Promise.resolve({data:100}));
            getDataStub.withArgs('/consumers/group1/offsets/topic1/partition1').returns(Promise.reject('testing no commited offset'));

            getDataStub.withArgs('/consumers/group1/owners/topic1/partition0').returns(Promise.resolve({data:'owner1'}));
            getDataStub.withArgs('/consumers/group1/owners/topic1/partition1').returns(Promise.reject('testing no owner assigned'));

            var expectedResult = [ [
                { consumer: 'group1', topic: 'topic1', partition: 'partition0', offset: '100', owner: 'owner1' },
                { consumer: 'group1', topic: 'topic1', partition: 'partition1', offset: undefined, owner: undefined }
            ], [
                { consumer: 'group2', topic: '', partition: '' }
            ] ];

            return libZookeeper.getConsumers().then(function(result){
                testResult(expectedResult, result);
            });
        });
    });
});
