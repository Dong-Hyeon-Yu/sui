// SPDX-License-Identifier: MIT 
pragma solidity >=0.4.0 <0.7.0;

contract SmallBank {
    
    //uint constant MAX_ACCOUNT = 10000;
    //uint constant BALANCE = 10000;
    //bytes20 constant accountTab = "account";
    //bytes20 constant savingTab = "saving";
    //bytes20 constant checkingTab = "checking";
    
    mapping(string=>uint) savingStore;
    mapping(string=>uint) checkingStore;

    function createAccount(string memory acc, uint initCheck, uint initSave) public {
        savingStore[acc] = initSave;
        checkingStore[acc] = initCheck;
    }

    function amalgamate(string memory dest, string memory src) public {
       uint bal1 = savingStore[dest];
       uint bal2 = checkingStore[src];
       
       savingStore[dest] = 0;
       checkingStore[src] = bal1 + bal2;
    }

    function getBalance(string memory acc) public view returns (uint balance) {
        uint bal1 = savingStore[acc];
        uint bal2 = checkingStore[acc];
        
        balance = bal1 + bal2;
        return balance;
    }
    
    function depositChecking(string memory acc, uint amount) public {
        uint bal1 = checkingStore[acc];
        
        checkingStore[acc] = bal1 + amount;
    }
    
    function updateSaving(string memory acc, uint amount) public {
        uint bal1 = savingStore[acc];
        
        savingStore[acc] = bal1 + amount;
    }
    
    function sendPayment(string memory dest, string memory src, uint amount) public {
        uint bal1 = checkingStore[dest];
        uint bal2 = checkingStore[src];
        
        bal1 -= amount;
        bal2 += amount;
        
        checkingStore[dest] = bal1;
        checkingStore[src] = bal2;
    }
    
    function writeCheck(string memory acc, uint amount) public {
        uint bal1 = checkingStore[acc];

        checkingStore[acc] = bal1 - amount;
    }
}