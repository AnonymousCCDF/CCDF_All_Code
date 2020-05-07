clear;
close all;

% load('.mat')
f = @(x)1*theta(1,1) + x(1)*theta(2,1) + x(2)*theta(3,1) + x(3)*theta(4,1) + x(4)*theta(5,1) + x(5)*theta(6,1) + x(6)*theta(7,1);
uoribound = [1;4;1;8;9;5];
luribound = [0.1;1;0.2;0;5;1];


% f = @(x)1*theta(1,1) + x(1)*theta(2,1) + x(2)*theta(3,1) + x(3)*theta(4,1) + x(4)*theta(5,1) + x(5)*theta(6,1) + x(6)*theta(7,1) + x(7)*theta(8,1);
% uoribound = [3;4;4;1;8;9;5];
% luribound = [1;1;1;0.2;0;5;1];


x0 = X_poly(1,2:end);
[~,n] = size(x0);
for j = 1:1:n
    ub(:,j) = (uoribound(j,:) - mu(1,j))/sigma(1,j);
    lb(:,j) = (luribound(j,:) - mu(1,j))/sigma(1,j);
end

[x,fval]=fmincon(f,x0,[],[],[],[],lb,ub)


[~,n] = size(x);
for j = 1:1:n
    x_ori(:,j) = x(:,j)* sigma(1,j) + mu(1,j);    
end

x_ori
