a = 2; c = 1; A = 2;
alpha = 0.602; r = 0.101;
ck = c./((k+1).^r); ak = a./((k+1+A).^alpha);

% measurement results are feed in this program to calculate
% theta and gradient in each itertaion

% iter = ;
% theta = [];

delta = randi([0,1],1,7);    
for i = 1:1:7
    if(delta(i) == 0) 
        delta(i) = -1;
    end
end
thetaP = theta +ck(iter)*delta;
thetaM = theta - ck(iter)*delta;

% yP and yM are the measurement result y(thetaP) and y(thetaM)
% yP = ; yM = ;

gradient = (yP-yM)/2/ck(iter)./delta * 10;

theta = theta - ak(iter)*gradient;

